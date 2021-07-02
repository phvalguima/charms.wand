"""

Basis class for the Manager relations.


Very often, we need to update and manage all the relations available for a
given endpoint. Managers are the objects that will abstract that work.

The relation managers are important because charms may have several copies
of the same relation (e.g. prometheus connected to several different apps).
Managers will work with those relations in a batch-like approach, running
them in for loops to update/read data from every relation available.

Implements basic proprties that every relation may need, such as spaces,
unit, its own charm, etc.
Besides, implements the logic to send data only if there is a difference
between the value already present and the newly proposed. That avoids
an infinite wave of -changed events containing the very same values.

The send()/send_app() methods will send data if a new value is proposed.
Besides, it sends to every relation available under the manager.

Managers focus on send() logic but not the read() equivalent. The reason
being that (1) there is not much to do besides actually reading the data
and (2) the data needs to be type-casted to its own objects from a string,
which only each Manager subclass will know how to do.

"""

import json
from ops.framework import Object


class RelationManagerBase(Object):

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._unit = charm.unit
        self._charm = charm
        self._app = charm.app
        self._relation_name = relation_name

    @property
    def app(self):
        return self._app

    @property
    def unit(self):
        return self._unit

    @property
    def relation(self):
        if not self.relations:
            return None
        return self.relations[0]

    @property
    def peer_addresses(self):
        addresses = []
        for u in self.relation.units:
            addresses.append(str(self.relation.data[u]["ingress-address"]))
        return addresses

    @property
    def advertise_addr(self):
        m = self.model
        return str(m.get_binding(self._relation_name).network.ingress_address)

    @property
    def binding_addr(self):
        m = self.model
        return str(m.get_binding(self._relation_name).network.bind_address)

    @property
    def relations(self):
        return self.framework.model.relations[self._relation_name]

    def send_app(self, field, value, rel=None):
        return self.send(field, value, rel, is_app=True)

    def send(self, field, value, rel=None, is_app=False):
        """Sends data to the relation.
        But before: (1) converts the data to string; and (2)
        check if the existing value is the same as the new one.
        If it is the same, then returns False and nothing is actually
        done. That avoids a look of -changed events happening with
        the same value being passed.

        rel: send can target one single relation if rel is set instead
        of all the relations available for this manager.

        is_app: This will allow to gate if we want to update app-wise
        or just the unit data relation. If the unit is not a leader,
        than the send will return False.
        """
        if not self.relations and not rel:
            return False
        # Either we iterate over all the relations or on specific relation
        relation = self.relations if not rel else [rel]
        if is_app and not self.unit.is_leader():
            # We cannot update an App without being the leader
            return False
        # Select which is the case, update app or unit
        rel_obj = self.app if is_app else self.unit

        # Now, deal with the value part
        # The thing to watch out is if the value is dict type
        # in this case, convert it to string using json.dumps()
        if isinstance(value, dict):
            v = json.dumps(value)
        else:
            v = str(value)
        result = False
        # Now, we have the data converted to string and we will try
        # update every relation available.
        for r in relation:
            if v == r.data[rel_obj].get(field, ""):
                continue
            r.data[rel_obj][field] = v
            result = True
        return result
