
__author__ = 'Kemele M. Endris'

from typing import List

from awudima.pysparql import SPARQL as utils
from awudima.pysparql import queryParser
from awudima.pysparql import Triple
from awudima.pysparql import Filter, Optional
from awudima.mediator.utilities import *


class AwudimaDecomposer(object):
    def __init__(self, query, config, pushdownssqjoins=True):
        if isinstance(query, str):
            self.query = queryParser.parse(query)
        else:
            self.query = query
        self.__original_query = str(query)
        self.prefixes = utils.getPrefs(self.query.prefs)
        self.config = config
        self.relevant_mts = {}
        self.source_selection = {}
        self.decomposed_query = None
        self.pushdownssqjoins = pushdownssqjoins

    def select_sources(self):
        if self.query is None:
            return None
        self.source_selection = self.decomposeUnionBlock(self.query.body)
        return self.source_selection

    def create_plan_tree(self, decomp):
        unionblocks = []
        sblocks = []
        opblocks = []
        for ub in decomp:
            BGP = ub['BGP']
            joinplans, non_match_filters = decompose_block(BGP,
                                                           ub['Filter'],
                                                           self.config,
                                                           isTreeBlock=True,
                                                           pushdownjoins=self.pushdownssqjoins)
            # from ontario.translators.sql.translator import SQLTranslator
            # for s in joinplans:
            #     if isinstance(s, Service):
            #         translator = SQLTranslator(self.config, s, self.query)
            #         translator.translate()
            #     elif isinstance(s, UnionBlock):
            #         for jb in s.triples:
            #             for sr in jb.triples:
            #                 translator = SQLTranslator(self.config, sr.service, self.query)
            #                 translator.translate()

            if len(ub['JoinBlock']) > 0:
                joinBlock = self.create_plan_tree(ub['JoinBlock'])
                sblocks.append(JoinBlock(joinBlock))
            if len(ub['UnionBlock']) > 0:
                unionBlock = self.create_plan_tree(ub['UnionBlock'])
                sblocks.append(UnionBlock(unionBlock))

            if len(ub['Optional']) > 0:
                # opblocks.append(Optional(UnionBlock(self.create_plan_tree(ub['Optional']))))
                optblks = self.create_plan_tree(ub['Optional'])
                for b in optblks:
                    opblocks.append(Optional(UnionBlock([b])))

            bgp_triples = []
            for s in BGP['stars']:
                bgp_triples.extend(BGP['stars'][s]['triples'])
            for s in sblocks:
                joinplans.append(s)

            matchedfilter = get_filters(bgp_triples, non_match_filters)
            gp = makeBushyTree(joinplans, matchedfilter)

            gp = [gp] + opblocks
            jfilter = []
            for f in non_match_filters:
                if f not in matchedfilter:
                    jfilter.append(f)

            gp = JoinBlock(gp, filters=jfilter)
            unionblocks.append(gp)

        return unionblocks

    def decompose(self):
        """
        Creates a Logical query plan for the given decomposed query

        :return: a (logical) bushy tree plan
        """
        if len(self.source_selection) == 0:
            self.select_sources()

        if self.source_selection is None:
            print("No data source is selected!")
            return None

        tree = self.create_plan_tree(self.source_selection)
        body = UnionBlock(tree)
        query = queryParser.parse(self.__original_query)
        query.body = body
        self.decomposed_query = query
        return self.decomposed_query

    def decomposeUnionBlock(self, ub):
        """
        Decompose a UnionBlock of a SPARQL query

        :param ub: UnionBlock
        :return:
        """
        r = []
        for jb in ub.triples:
            pjb = self.decomposeJoinBlock(jb)
            if pjb:
                r.append(pjb)
            else:
                return []
        return r

    def decomposeJoinBlock(self, jb):
        """
        select_sources a Join Block into a set of star-shaped subqueries and find a matching RDF Molecule Templates
        :param jb: JoinBlock
        :return:
        """
        tl = []
        ol = []
        ijb = []
        ub = []
        fl = []
        for bgp in jb.triples:
            if isinstance(bgp, Triple):
                tl.append(bgp)
            elif isinstance(bgp, Filter):
                fl.append(bgp)
            elif isinstance(bgp, Optional):
                ubb = self.decomposeUnionBlock(bgp.bgg)
                ol.extend(ubb)
            elif isinstance(bgp, UnionBlock):
                pub = self.decomposeUnionBlock(bgp)
                if pub:
                    ub.extend(pub)
            elif isinstance(bgp, JoinBlock):
                pub = self.decomposeJoinBlock(bgp)
                if pub:
                    ijb.extend(pub)

        tl_bgp = {'stars_conn': {},
                  'mts_conn': {},
                  'stars': {},
                  'bgp_predicates': []}

        if tl is not None and len(tl) > 0:
            bgp_preds = self.get_preds(tl)
            stars = self.bgp_stars(tl)
            bgpstars, star_conn, mt_conn = self.decompose_bgp(stars, bgp_preds)
            if len(bgpstars) == 0:
                return None
            tl_bgp['stars_conn'] = star_conn
            tl_bgp['mts_conn'] = mt_conn
            tl_bgp['stars'] = bgpstars
            tl_bgp['bgp_predicates'] = bgp_preds
            tl_bgp['variables'] = self.get_vars(tl)

        return {
            "BGP": tl_bgp,
            "Optional": ol,
            "JoinBlock": ijb,
            "UnionBlock": ub,
            "Filter": fl
        }

    def get_vars(self, bgp: List[Triple]):
        """
        Returns a list of variables from a `BGP` (list of `Triples`)

        :param bgp: basic graph pattern (BGP) as a list of `Triple`s
        :return: list of variables (as list of strings)
        """
        variables = []
        for tp in bgp:
            variables.extend(tp.getVars())

        return list(set(variables))

    def get_preds(self, star):
        """
        Returns a set of predicates in a BGP/star-shaped subquery
        :param star: list of triple patterns
        :return: list of predicates
        """
        return [utils.getUri(tr.predicate, self.prefixes)[1:-1] for tr in star if tr.predicate.constant]

    def get_pred_objs(self, star):
        """
        Returns a key value of predicate:object in a BGP/star-shaped subquery
        :param star: list of triple patterns
        :return: list of predicates
        """
        preds = {}
        for tr in star:
            if tr.predicate.constant:
                preds.setdefault(utils.getUri(tr.predicate, self.prefixes)[1:-1], []).append((utils.getUri(tr.theobject, self.prefixes)
                      if tr.theobject.constant else tr.theobject.name))

        return preds

    def bgp_stars(self, bgp):
        """
        Split BGP to a set of Star-shaped Subqueries and return a dictionary of stars' subject part to set of triples:
            stars = {'?s1': [tp1, tp2, ..], '?s2':[tp3, ..], ..}
        :param bgp: Basic Graph Pattern of a SPARQL query
        :return: stars = {'?s1': [tp1, tp2, ..], '?s2':[tp3, ..], ..}
        """
        stars = {}

        for tp in bgp:
            subj = tp.subject.name
            if subj in stars:
                stars[subj].append(tp)
            else:
                stars[subj] = [tp]

        return stars

    def getStarsConnections(self, stars):
        """
        extracts links between star-shaped sub-queries
        :param stars: map of star-shaped sub-queries with its root (subject) {subject: [triples in BGP]}
        :return: map of star-shaped sub-query root name (subject) with its connected sub-queries via its object node.
         {subj1: [subjn]} where one of subj1's triple pattern's object node is connected to subject node of subjn
        """
        conn = dict()
        star_objs = {}
        for s in stars:
            objs = [t.theobject.name for t in stars[s] if not t.theobject.constant]
            star_objs[s] = objs
            conn[s] = {"SO": [], "OO": []}

        subjects = list(set(stars.keys()))
        checked = []
        for s in star_objs:
            connections = set(star_objs[s]).intersection(subjects)
            if len(connections) > 0:
                for c in connections:
                    conn[c]['SO'].append(s)
            for s2 in star_objs:
                if s == s2:
                    continue
                if s2 + s not in checked and s + s2 not in checked:
                    connections = set(star_objs[s]).intersection(star_objs[s2])
                    if len(connections) > 0:
                        # for c in connections:
                        conn[s2]['OO'].append(s)
                        conn[s]['OO'].append(s2)
                checked.extend([s2 + s, s + s2])

        return conn

    def getMTsConnection(self, selectedmolecules, preds, relevant_mts):
        mcons = {}
        smolecules = [m for s in selectedmolecules for m in selectedmolecules[s]]
        for s in selectedmolecules:
            mols = selectedmolecules[s]
            for m in mols:
                mcons[m] = []
                mtpreds = relevant_mts[m].preds_as_dict_obj
                for prId, pred in mtpreds.items():
                    for r in pred.ranges:
                        if r in smolecules and prId in preds:
                            mcons[m].append(r)
        return mcons

    def checkRDFTypeStatemnt(self, ltr):
        types = self.getRDFTypeStatement(ltr)
        typemols = {}
        for t in types:
            tt = utils.getUri(t.theobject, self.prefixes)[1:-1]
            if tt in self.config.rdfmts_obj:
                mt = self.config.rdfmts_obj[tt]
                typemols[tt] = mt
        if len(types) > 0 and len(typemols) == 0:
            return {}

        return typemols

    def getRDFTypeStatement(self, ltr):
        types = []
        for t in ltr:
            if t.predicate.constant \
                    and (t.predicate.name == "a"
                         or t.predicate.name == "rdf:type"
                         or t.predicate.name == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") \
                    and t.theobject.constant:
                types.append(t)

        return types

    def prune(self, star_conn, res_conn, selectedmolecules, stars, relevant_mts):
        newselected = {}
        res = {}
        counter = 0
        for s in selectedmolecules:
            if len(selectedmolecules[s]) == 1:
                newselected[s] = list(selectedmolecules[s])
                res[s] = list(selectedmolecules[s])
                counter += 1
            else:
                newselected[s] = []
                res[s] = []
        if counter == len(selectedmolecules):
            return res

        # check predicate level connections
        newfilteredonly = {}
        for s in res:
            sc = [c for c in star_conn if s in star_conn[c]['SO']]
            for c in sc:
                connectingtp = [utils.getUri(tp.predicate, self.prefixes)[1:-1]
                                for tp in stars[s] if tp.theobject.name == c]
                connectingtp = list(set(connectingtp))
                sm = selectedmolecules[s]
                for m in sm:
                    mtpreds = relevant_mts[m].preds_as_dict_obj
                    srange = []
                    for prId, pred in mtpreds.items():
                        for r in pred.ranges:
                            if prId in connectingtp:
                                srange.append(r)

                    # srange = [p for r in relevant_mts[m].predicates
                    #           for p in relevant_mts[m].predicates[r].ranges
                    #           if relevant_mts[m].predicates[r].predicate in connectingtp]
                    srange = list(set(srange).intersection(selectedmolecules[c]))
                    # if len(srange) == 0:
                    #     selectedmolecules[s].remove(m)
                    if c in newfilteredonly:
                        newfilteredonly[c].extend(srange)
                    else:
                        newfilteredonly[c] = srange
                    newfilteredonly[c] = list(set(newfilteredonly[c]))

        already_checked = []
        for s in res:
            sc = [c for c in star_conn if s in star_conn[c]['SO']]
            for c in sc:
                if s + c in already_checked or c + s in already_checked:
                    continue

                already_checked.extend([s + c, c + s])
                if c in newfilteredonly:
                    c_newfilter = newfilteredonly[c].copy()
                else:
                    c_newfilter = selectedmolecules[c].copy()
                    newfilteredonly[c] = selectedmolecules[c].copy()
                if s in newfilteredonly:
                    s_newfilter = newfilteredonly[s].copy()
                else:
                    s_newfilter = selectedmolecules[s].copy()
                    newfilteredonly[s] = selectedmolecules[s].copy()
                for m in s_newfilter:
                    con = res_conn[m]
                    if len(con) == 0:
                        continue
                    new_res = list(set(con).intersection(c_newfilter))
                    if len(new_res) == 0:
                        newfilteredonly[s].remove(m)
                    else:
                        newfilteredonly[c] = new_res

                # for m in c_newfilter:
                #     con = res_conn[m]
                #     if len(con) == 0:
                #         continue
                #     new_res = list(set(con).intersection(s_newfilter))
                #     if len(new_res) == 0:
                #         newfilteredonly[c].remove(m)

        for s in newfilteredonly:
            res[s] = list(set(newfilteredonly[s]))

        for s in res:
            if len(res[s]) == 0:
                res[s] = selectedmolecules[s]
            res[s] = list(set(res[s]))
        return res

    def find_rdfmt_by_preds(self, preds):
        rdfmts = None
        for p in preds:
            predIdx = self.config.predicate_rdfmts
            if p not in predIdx:
                return {}
            if rdfmts is None:
                rdfmts = set(predIdx[p])
            else:
                rdfmts = rdfmts.intersection(predIdx[p])

        if rdfmts is not None:
            return {mt: self.config.rdfmts_obj[mt] for mt in rdfmts}

        return rdfmts

    def decompose_bgp(self, stars, bgp_preds):
        bgpstars = {}
        mtres = {}
        relevant_mts = {}
        starnames = sorted(list(stars.keys()))
        for s in starnames:
            spred = self.get_pred_objs(stars[s])
            bgpstars[s] = {}

            bgpstars[s]['triples'] = sorted(stars[s])

            bgpstars[s]['predicates'] = spred
            types = self.checkRDFTypeStatemnt(stars[s])
            if len(types) > 0:
                rdfmts = types
            else:
                rdfmts = self.find_rdfmt_by_preds(spred)

            bgpstars[s]['rdfmts'] = list(rdfmts.keys())
            mtres[s] = bgpstars[s]['rdfmts']
            relevant_mts.update(rdfmts)
        star_conn = self.getStarsConnections(stars)
        mt_conn = self.getMTsConnection(mtres, bgp_preds, relevant_mts)
        res = self.prune(star_conn, mt_conn, mtres, stars, relevant_mts)

        for s in res:
            bgpstars[s]['rdfmts'] = res[s]

        for s in res:
            datasources = {}
            for m in res[s]:
                molecule = self.config.rdfmts_obj[m]
                for d in molecule.datasources:
                    dspreds = {pred.predId: pred for pred in molecule.predicate_sources[d.dsId]}
                    preds = list(set(bgpstars[s]['predicates']).intersection(list(dspreds.keys())))
                    if len(preds) > 0:
                        datasources.setdefault(d.dsId, {}).setdefault(m, []).extend([dspreds[op] for op in preds])
                    else:
                        datasources.setdefault(d.dsId, {}).setdefault(m, []).extend(list(dspreds.values()))
                        if len(bgpstars[s]['predicates']) == 0:
                            preds = {tr.predicate.name: (utils.getUri(tr.theobject, self.prefixes) if tr.theobject.constant else tr.theobject.name)
                                     for tr in stars[s]}
                            bgpstars[s]['predicates'] = preds
            if len(datasources) == 0:
                return [], [], []
            bgpstars[s]['datasources'] = datasources
            bgpstars[s]['variables'] = self.get_vars(bgpstars[s]['triples'])

        return bgpstars, star_conn, mt_conn

    # def create_decomposed_query(self, decomp):
    #     unionblocks = []
    #     sblocks = []
    #     opblocks = []
    #     for ub in decomp:
    #         BGP = ub['BGP']
    #         joinplans = decompose_block(BGP, ub['Filter'], self.config, isTreeBlock=False)
    #
    #         if len(ub['JoinBlock']) > 0:
    #             joinBlock = self.create_decomposed_query(ub['JoinBlock'])
    #             sblocks.append(JoinBlock(joinBlock))
    #         if len(ub['UnionBlock']) > 0:
    #             unionBlock = self.create_decomposed_query(ub['UnionBlock'])
    #             sblocks.append(UnionBlock(unionBlock))
    #
    #         if len(ub['Optional']) > 0:
    #             opblocks.append(Optional(UnionBlock(self.create_decomposed_query(ub['Optional']))))
    #
    #         gp = joinplans + sblocks + opblocks
    #         gp = UnionBlock([JoinBlock(gp)])
    #         unionblocks.append(gp)
    #
    #     return unionblocks

    '''
        ===================================================
        ========= FILTERS =================================
        ===================================================
        '''

    def includeFilter(self, jb_triples, fl):
        fl1 = []
        for jb in jb_triples:

            if isinstance(jb, list):
                for f in fl:
                    fl2 = self.includeFilterAux(f, jb)
                    fl1 = fl1 + fl2
            elif isinstance(jb, UnionBlock):
                for f in fl:
                    fl2 = self.includeFilterUnionBlock(jb, f)
                    fl1 = fl1 + fl2
            elif isinstance(jb, Service):
                for f in fl:
                    fl2 = self.includeFilterAuxSK(f, jb.triples, jb)
                    fl1 = fl1 + fl2
        return fl1

    def includeFilterAux(self, f, sl):
        fl1 = []
        for s in sl:
            vars_s = set()
            for t in s.triples:
                vars_s.update(set(utils.getVars(t)))
            vars_f = f.getVars()
            if set(vars_s) & set(vars_f) == set(vars_f):
                s.include_filter(f)
                fl1 = fl1 + [f]
        return fl1

    def includeFilterUnionBlock(self, jb, f):
        fl1 = []
        for jbJ in jb.triples:
            for jbUS in jbJ.triples:
                if isinstance(jbUS, Service):
                    vars_s = set(jbUS.getVars())
                    vars_f = f.getVars()
                    if set(vars_s) & set(vars_f) == set(vars_f):
                        jbUS.include_filter(f)
                        fl1 = fl1 + [f]
        return fl1

    def includeFilterAuxSK(self, f, sl, sr):
        """
        updated: includeFilterAuxS(f, sl, sr) below to include filters that all vars in filter exists in any of the triple
        patterns of a BGP. the previous impl includes them only if all vars are in a single triple pattern
        :param f:
        :param sl:
        :param sr:
        :return:
        """
        fl1 = []
        serviceFilter = False
        fvars = dict()
        vars_f = f.getVars()

        for v in vars_f:
            fvars[v] = False
        bgpvars = set()

        for s in sl:
            bgpvars.update(set(utils.getVars(s)))
            vars_s = set()
            if isinstance(s, Triple):
                vars_s.update(set(utils.getVars(s)))
            else:
                for t in s.triples:
                    vars_s.update(set(utils.getVars(t)))

            if set(vars_s) & set(vars_f) == set(vars_f):
                serviceFilter = True

        for v in bgpvars:
            if v in fvars:
                fvars[v] = True
        if serviceFilter:
            sr.include_filter(f)
            fl1 = fl1 + [f]
        else:
            fs = [v for v in fvars if not fvars[v]]
            if len(fs) == 0:
                sr.include_filter(f)
                fl1 = fl1 + [f]
        return fl1

    def updateFilters(self, node, filters):
        return UnionBlock(node.triples, filters)

#
# def get_filters(triples, filters):
#     result = []
#     t_vars = []
#     for t in triples:
#         t_vars.extend(t.getVars())
#
#     for f in filters:
#         f_vars = f.getVars()
#         if len(set(f_vars).intersection(t_vars)) == len(set(f_vars)):
#             result.append(f)
#
#     return result
#
#
