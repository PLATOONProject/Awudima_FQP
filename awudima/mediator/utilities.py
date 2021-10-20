
from awudima.pyrml import DataSourceType
from awudima.pysparql import Service, UnionBlock, JoinBlock
from awudima.pyrdfmt import Federation
from awudima.mediator.LogicalPlan import makeBushyTree


def get_filters(triples, filters):
    result = []
    t_vars = []
    for t in triples:
        t_vars.extend(t.getVars())

    for f in filters:
        f_vars = f.getVars()
        if len(set(f_vars).intersection(t_vars)) == len(set(f_vars)):
            result.append(f)

    return result


def push_down_join(services):
    new_services = []
    endpoints = [s.endpoint for s in services
                 if s.datasource.dstype == DataSourceType.SPARQL_ENDPOINT or
                 s.datasource.dstype == DataSourceType.MYSQL or
                 s.datasource.dstype == DataSourceType.MONGODB_LD_FLAT]
    starsendp = {}
    services_to_remove = []
    for e in set(endpoints):
        servs = list(set([s for s in services if s.endpoint == e]))
        starsendp[e] = servs
        others = []
        while len(servs) > 1:
            done = False
            l = servs.pop(0)  # heapq.heappop(pq)
            lpq = servs  # heapq.nsmallest(len(pq), pq)

            for i in range(0, len(servs)):
                r = lpq[i]
                if len(set(l.getVars()) & set(r.getVars())) > 0:
                    servs.remove(r)
                    stars = {}
                    for svar, triples in l.stars.items():
                        stars[svar] = triples

                    assigned = False
                    for svar, triples in r.stars.items():
                        stars[svar] = triples
                        if not assigned:
                            dsid = l.datasource.dsId
                            stars[svar]['datasources'] = {dsid: stars[svar]['datasources'][dsid]}
                            assigned = True

                    # stars = l.stars
                    # stars.update(r.stars)
                    star_filters = l.star_filters.copy()
                    star_filters.update(r.star_filters)

                    star_triples = []
                    star_triples.extend(l.triples)
                    star_triples.extend(r.triples)
                    molecules = list(set(l.rdfmts + r.rdfmts))
                    mms = "|-|".join(molecules)
                    new_service = Service(endpoint=l.datasource.dsId + '@' + l.datasource.url + '@' + mms,
                                          triples=star_triples,
                                          datasource=l.datasource,
                                          rdfmts=molecules,
                                          stars=stars,
                                          filters=list(set(l.filters + r.filters)),
                                          star_filters=star_filters)
                    servs.append(new_service)
                    done = True
                    services_to_remove.append(l)
                    services_to_remove.append(r)

                    break
            if not done:
                others.append(l)
        if len(servs) == 1:
            new_services.append(servs[0])
            new_services.extend(others)
        elif others:
            new_services.extend(others)
        for s in services_to_remove:
            if s in services:
                services.remove(s)

    for s in new_services:
        if s not in services:
            services.append(s)
    return services


def decompose_block(BGP, filters, config: Federation, isTreeBlock=False, pushdownjoins=True):
    joinplans = []
    services = []
    filter_pushed = False
    non_match_filters = []
    ssqs = list(BGP['stars'].keys())
    ssqs = sorted(ssqs)
    for s in ssqs:
        star = BGP['stars'][s]
        dss = star['datasources']
        preds = star['predicates']
        sources = set()
        star_filters = get_filters(list(set(star['triples'])), filters)
        for ID, rdfmt in dss.items():
            for mt, mtpred in rdfmt.items():
                ppred = [p for p in preds if '?' not in p and p != '']
                if len(set(preds).intersection(
                        [p.predId for p in mtpred] + ['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'])) == len(set(preds)) or len \
                    (ppred) == 0:
                    sources.add(ID)
                    break

        if len(sources) > 1:
            sources = sorted(sources)

            elems = []
            for d in sources:
                stars = star.copy()
                stars['datasources'] = {d: stars['datasources'][d]}
                stars = {s: stars}
                service = Service(
                    endpoint=d + '@' + config.datasources_obj[d].url,
                    triples=star['triples'],
                    datasource=config.datasources_obj[d],
                    rdfmts=list(star['datasources'][d].keys()),
                    stars=stars,
                    filters=star_filters,
                    star_filters={s: star_filters})
                if isTreeBlock:
                    elems.append(JoinBlock([makeBushyTree([service])]))
                else:
                    elems.append(JoinBlock(([service])))

            ubl = UnionBlock(elems)
            joinplans = joinplans + [ubl]
        elif len(sources) == 1:
            d = sources.pop()
            stars = star.copy()
            stars['datasources'] = {d: stars['datasources'][d]}
            stars = {s: stars}
            serv = Service(endpoint=d + '@' + config.datasources_obj[d].url,
                           triples=star['triples'],
                           datasource=config.datasources_obj[d],
                           rdfmts=star['rdfmts'],
                           stars=stars,
                           filters=star_filters,
                           star_filters={s: star_filters})
            services.append(serv)

        if len(filters) == len(star_filters):
            filter_pushed = True
        else:
            non_match_filters = list(set(filters).difference(star_filters))
    if pushdownjoins:
        services = push_down_join(services)
    if services and joinplans:
        joinplans = services + joinplans
    elif services:
        joinplans = services

    # joinplans = makeBushyTree(joinplans, filters)

    return joinplans, non_match_filters if not filter_pushed else []

