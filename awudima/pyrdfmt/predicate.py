
__author__ = "Kemele M. Endris"


class MTPredicate(object):
    """Represents predicates of an RDF molecule template

       A predicate/property of a molecule template represents a single data point associated to molecule (an instance of a
       molecule template.)
       """
    def __init__(self, predId, label, desc='', cardinality=-1):
        """

        :param predId: uri/id of the predicate
        :param label: name/label of the predicate
        :param desc:
        :param cardinality:
        """

        self.predId = predId
        self.label = label
        self.desc = desc
        self.ranges = set()
        self.cardinality = cardinality
        self.prefix = ''
        self.constraints = []
        self.policy = None

    def to_str(self):
        """Produces a textual representation of the predicate

        :return: text representation as predId(label)
        """

        return self.predId

    def to_json(self):
        """Produces a JSON representation of the predicate

        :return: json representation of the predicate
        """

        return {
            "predId": self.predId,
            'label': self.label,
            'desc': self.desc,
            'cardinality': self.cardinality,
            'prefix': self.prefix,
            "ranges": list(self.ranges),
            "constraints": list(self.constraints),
            "policy": self.policy
        }

    def merge_with(self, other):
        if self.predId != other.predId:
            raise Exception("Cannot merge two different Predicates " + self.predId + ' and ' + other.predId)
        merged = MTPredicate(self.predId, self.label, self.desc, self.cardinality)
        if self.label is None or len(self.label) == 0:
            merged.label = other.label
        if self.desc is None or len(self.desc) == 0:
            merged.desc = other.desc
        if self.cardinality == -1:
            merged.cardinality = other.cardinality
        else:
            try:
                merged.cardinality = int(merged.cardinality) + int(other.cardinality)
            except:
                pass

        merged.ranges = set(list(self.ranges) + list(other.ranges))
        # TODO: merge constraints and polity (restriced first approach)

        return merged

    def addRanges(self, ranges):
        self.ranges.update(ranges)

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __eq__(self, other):
        return self.predId == other.predId

    def __hash__(self):
        return hash(self.predId)

    @staticmethod
    def load_from_json(predicates):
        preds = []
        for p in predicates:
            if 'predId' not in p:
                continue
            lbl = p['label'] if 'label' in p else ''
            desc = p['desc'] if 'desc' in p else ''
            pred = MTPredicate(p['predId'], lbl, desc)
            if 'cardinality' in p:
                pred.cardinality = p['cardinality']
            # if '^^' in str(pred.cardinality):
            #     pred.cardinality = pred.cardinality[:pred.cardinality.find('^^')]
            if 'ranges' in p:
                pred.ranges = set(p['ranges'])
            if 'policy' in p:
                pred.policy = p['policy']
            if 'constraints' in p:
                pred.constraints = p['constraints']
            if 'prefix' in p:
                pred.prefix = p['prefix']

            preds.append(pred)
        return preds
