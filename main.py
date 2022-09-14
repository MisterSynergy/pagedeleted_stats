from os.path import expanduser
from time import time, sleep, strftime
from io import StringIO
import pandas as pd
import requests
import pywikibot as pwb


WIKIDATA_API_ENDPOINT = 'https://www.wikidata.org/w/api.php'
WDQS_ENDPOINT = 'https://query.wikidata.org/sparql'
PAGEDELETED_ROOT = 'Pasleim/Items_for_deletion/Page_deleted'
REPORT_PAGE = 'User:MisterSynergy/sysop/pagedeleted stats'
REPORT_EDIT_SUMMARY = 'update statistics (weekly job via Toolforge) #msynbot #unapproved'
USER_AGENT = f'{requests.utils.default_headers()["User-Agent"]} (Wikidata '\
              'bot by User:MisterSynergy; mailto:mister.synergy@yahoo.com)'
API_HEADERS = { 'User-Agent' : USER_AGENT }
WDQS_HEADERS = { 'User-Agent' : USER_AGENT, 'Accept' : 'text/csv' }
WDQS_SLEEP = 5
TOOL_PATH = '/pywikibot_tasks/pagedeleted_stats'
REPORT_TEMPLATE = f'{expanduser("~")}{TOOL_PATH}/static/report_template.txt'
QUERY_TEMPLATE_FILENAME = f'{expanduser("~")}{TOOL_PATH}/queries/{{name}}.rq'
TEMPLOG_FILENAME = f'{expanduser("~")}{TOOL_PATH}/pagedeleted_templog.txt'


def query_pagedeleted_pages() -> list[str]:
    payload = requests.post(
        WIKIDATA_API_ENDPOINT,
        data={
            'action' : 'query',
            'list' : 'allpages',
            'apnamespace' : '2',
            'aplimit' : '50',
            'apprefix' : PAGEDELETED_ROOT,
            'format' : 'json'
        },
        headers=API_HEADERS
    ).json()

    pages = []
    for elem in payload.get('query', {}).get('allpages', []):
        pages.append(elem.get('title'))

    return pages


def query_wdqs_to_dataframe(query:str, column_names:list[str], \
                            dtype:dict[str, type]) -> pd.DataFrame:
    t_start = time()

    response = requests.post(
        url=WDQS_ENDPOINT,
        data={ 'query' : query },
        headers=WDQS_HEADERS
    )

    t_diff = time() - t_start
    sleep(WDQS_SLEEP)

    if response.status_code != 200:
        with open(TEMPLOG_FILENAME, mode='a', encoding='utf8') as file_handle:
            file_handle.write(f'{strftime("%Y-%m-%d %H:%M:%S")}: status {response.status_code}' \
                              f' for query {query} ({t_diff} s)\n\n')
        raise RuntimeWarning('Query not successful')

    dataframe = pd.read_csv(
        StringIO(response.text),
        header=0,
        names=column_names,
        dtype=dtype
    )

    return dataframe


def iter_pages(pages:list[str], query_name:str, column_names:list[str], \
               dtype:dict[str, type]) -> tuple[pd.DataFrame, list[str]]:
    query =  read_query_template(query_name)

    dataframes = []
    results_missing = []

    for page in pages:
        #print(query_name, ' -- ', page)
        try:
            dataframe = query_wdqs_to_dataframe(
                query.format(
                    pagetitle=page.replace(' ', '_')
                ),
                column_names,
                dtype
            )
        except RuntimeWarning:
            results_missing.append(page)
            continue
        else:
            dataframe['source'] = page
            dataframes.append(dataframe)

    payload = pd.concat(dataframes, ignore_index=True)

    return payload, results_missing


def make_wikitable(wikitable_rows:pd.Series, column_names:list[str]) -> str:
    wikitable = f"""{{| class="wikitable sortable"\n|-\n! {' !! '.join(column_names)}\n"""
    wikitable += ''.join(wikitable_rows)
    wikitable += "|}"

    return wikitable


def make_table_rows_1(row:pd.Series) -> str:
    if len(row.entity) > 0:
        table_row_template = '|-\n| {rank} || [[{entity}]] || {label} || {cnt}\n'
    else:
        table_row_template = "|-\n| {rank} || ''(none)'' || ''(none)'' || {cnt}\n"

    params = {
        'rank' : int(row['rank']),
        'cnt' : row['cnt'],
        'entity' : row['entity'],
        'label' : row['label']
    }

    return table_row_template.format(**params)


def make_table_rows_2(row:pd.Series) -> str:
    table_row_template = '|-\n| {rank} || [[{entity}]] || {label} || {type} || {cnt}\n'

    params = {
        'rank' : int(row['rank']),
        'cnt' : row['cnt'],
        'entity' : row['entity'],
        'label' : row['label'],
        'type' : row['type']
    }

    return table_row_template.format(**params)


def make_table_rows_3(row:pd.Series) -> str:
    if len(row.entity) > 0:
        table_row_template = '|-\n| {rank} || {predicate} || [[{entity}]] || {label} || {cnt}\n'
    else:
        table_row_template = '|-\n| {rank} || {predicate} || {predicate} || {predicate} || {cnt}\n'

    params = {
        'rank' : int(row['rank']),
        'cnt' : row['cnt'],
        'entity' : row['entity'],
        'label' : row['label'],
        'predicate' : row['predicate']
    }

    return table_row_template.format(**params)


def query_types(pages:list[str]) -> tuple[str, list[str]]:
    payload, results_missing = iter_pages(
        pages,
        'types',
        [ 'entity', 'label', 'cnt' ],
        {
            'entity' : str,
            'label' : str,
            'cnt' : int
        }
    )

    payload['entity'] = payload['entity'].str.replace(
        pat='http://www.wikidata.org/entity/',
        repl='',
        regex=False
    )
    payload.loc[payload['entity'].isna(), 'entity'] = ''
    payload.loc[payload['label'].isna(), 'label'] = '@' # as sortkey; not displayed

    sums = payload.groupby(by=['entity', 'label'], as_index=False).sum()
    sums.sort_values(by=[ 'cnt', 'label' ], ascending=[ False, True ], inplace=True)
    sums['rank'] = sums['cnt'].rank(method='first', ascending=False)
    sums['wikitable_row'] = sums.apply(func=make_table_rows_1, axis=1)

    wikitable = make_wikitable(sums['wikitable_row'], ['rank', 'item', 'label', 'count'])

    return wikitable, results_missing


def query_occupations(pages:list[str]) -> tuple[str, list[str]]:
    payload, results_missing = iter_pages(
        pages,
        'occupations',
        [ 'entity', 'label', 'cnt' ],
        {
            'entity' : str,
            'label' : str,
            'cnt' : int
        }
    )

    payload['entity'] = payload['entity'].str.replace(
        pat='http://www.wikidata.org/entity/',
        repl='',
        regex=False
    )
    payload.loc[payload['entity'].isna(), 'entity'] = ''
    payload.loc[payload['label'].isna(), 'label'] = '@' # as sortkey; not displayed

    sums = payload.groupby(by=['entity', 'label'], as_index=False).sum()
    sums.sort_values(by=[ 'cnt', 'label' ], ascending=[ False, True ], inplace=True)
    sums['rank'] = sums['cnt'].rank(method='first', ascending=False)
    sums['wikitable_row'] = sums.apply(func=make_table_rows_1, axis=1)

    wikitable = make_wikitable(sums['wikitable_row'], ['rank', 'item', 'label', 'count'])

    return wikitable, results_missing


def query_backlinks(pages:list[str]) -> tuple[str, list[str]]:
    payload, results_missing = iter_pages(
        pages,
        'backlinks',
        [ 'entity', 'label', 'predicate', 'cnt' ],
        {
            'entity' : str,
            'label' : str,
            'predicate' : str,
            'cnt' : int
        }
    )

    payload['entity'] = payload['entity'].str.replace(
        pat='http://www.wikidata.org/entity/',
        repl='',
        regex=False
    )
    predicate_replacements = {
        'http://www.wikidata.org/prop/statement/' : 'ps:',
        'http://www.wikidata.org/prop/qualifier/' : 'pq:',
        'http://www.wikidata.org/prop/reference/' : 'pr:',
        'http://www.w3.org/2002/07/owl#' : 'owl:',
        'http://schema.org/' : 'schema:',
        'http://wikiba.se/ontology#' : 'wikibase:'
    }
    for long, short in predicate_replacements.items():
        payload['predicate'] = payload['predicate'].str.replace(pat=long, repl=short, regex=False)

    payload.loc[payload['entity'].notna(), 'entity'] = 'Property:' + payload['entity']
    payload.loc[payload['entity'].isna(), 'entity'] = ''
    payload.loc[payload['label'].isna(), 'label'] = '@' # as sortkey; not displayed

    sums = payload.groupby(by=['entity', 'label', 'predicate'], as_index=False).sum()
    sums.sort_values(by=[ 'cnt', 'label' ], ascending=[ False, True ], inplace=True)
    sums['rank'] = sums['cnt'].rank(method='first', ascending=False)
    sums['wikitable_row'] = sums.apply(func=make_table_rows_3, axis=1)

    wikitable = make_wikitable(
        sums['wikitable_row'],
        ['rank', 'predicate', 'property', 'label', 'count']
    )

    return wikitable, results_missing


def query_identifiers(pages:list[str]) -> tuple[str, list[str]]:
    payload, results_missing = iter_pages(
        pages,
        'identifiers',
        [ 'entity', 'label', 'cnt' ],
        {
            'entity' : str,
            'label' : str,
            'cnt' : int
        }
    )

    payload['entity'] = payload['entity'].str.replace(
        pat='http://www.wikidata.org/entity/',
        repl='',
        regex=False
    )
    payload.loc[payload['entity'].notna(), 'entity'] = 'Property:' + payload['entity']

    sums = payload.groupby(by=['entity', 'label'], as_index=False).sum()
    sums.sort_values(by=[ 'cnt', 'label' ], ascending=[ False, True ], inplace=True)
    sums['rank'] = sums['cnt'].rank(method='first', ascending=False)
    sums['wikitable_row'] = sums.apply(func=make_table_rows_1, axis=1)

    wikitable = make_wikitable(sums['wikitable_row'], ['rank', 'property', 'label', 'count'])

    return wikitable, results_missing


def query_properties(pages:list[str]) -> tuple[str, list[str]]:
    payload, results_missing = iter_pages(
        pages,
        'properties',
        [ 'entity', 'label', 'type', 'cnt' ],
        {
            'entity' : str,
            'label' : str,
            'type' : str,
            'cnt' : int
        }
    )

    payload['entity'] = payload['entity'].str.replace(
        pat='http://www.wikidata.org/entity/',
        repl='',
        regex=False
    )
    payload['type'] = payload['type'].str.replace(
        pat='http://wikiba.se/ontology#',
        repl='wikibase:',
        regex=False
    )
    payload.loc[payload['entity'].notna(), 'entity'] = 'Property:' + payload['entity']

    sums = payload.groupby(by=['entity', 'label', 'type'], as_index=False).sum()
    sums.sort_values(by=[ 'cnt', 'label' ], ascending=[ False, True ], inplace=True)
    sums['rank'] = sums['cnt'].rank(method='first', ascending=False)
    sums['wikitable_row'] = sums.apply(func=make_table_rows_2, axis=1)

    wikitable = make_wikitable(
        sums['wikitable_row'],
        [ 'rank', 'property', 'label', 'type', 'count' ]
    )

    return wikitable, results_missing


def write_to_wiki(wikitext:str) -> None:
    site = pwb.Site(
        code='wikidata',
        fam='wikidata'
    )
    site.login()
    page = pwb.Page(
        site,
        REPORT_PAGE
    )
    page.text = wikitext
    page.save(
        summary=REPORT_EDIT_SUMMARY,
        watch='nochange',
        minor=True,
        quiet=True
    )


def missing_message(page_list:list[str]) -> str:
    if len(page_list) > 0:
        return 'Ignored due to query timeouts: [[' + ']], [['.join(page_list) + ']]\n'

    return ''


def clear_templog() -> None:
    open(TEMPLOG_FILENAME, mode='w', encoding='utf8').write('')


def read_query_template(name:str) -> str:
    with open(QUERY_TEMPLATE_FILENAME.format(name=name), mode='r', encoding='utf8') as file_handle:
        template = file_handle.read()

    return template


def read_report_template() -> str:
    with open(REPORT_TEMPLATE, mode='r', encoding='utf8') as file_handle:
        template = file_handle.read()

    return template


def main() -> None:
    clear_templog()
    pages = query_pagedeleted_pages()

    pagedeleted_missing = {}

    wikitable1, pagedeleted_missing['types'] = query_types(pages)
    wikitable2, pagedeleted_missing['occupations'] = query_occupations(pages)
    wikitable3, pagedeleted_missing['backlinks'] = query_backlinks(pages)
    wikitable4, pagedeleted_missing['identifiers'] = query_identifiers(pages)
    wikitable5, pagedeleted_missing['properties'] = query_properties(pages)

    #reporting = {
    #    'wt1' : wikitable1,
    #    'wt2' : wikitable2,
    #    'wt3' : wikitable3,
    #    'wt4' : wikitable4,
    #    'wt5' : wikitable5
    #}
    #for file_name, wikitable in reporting.items():
    #    with open(f'./{file_name}.txt', mode='w') as file_handle:
    #        file_handle.write(wikitable)

    write_to_wiki(
        read_report_template().format(
            wikitable1=wikitable1,
            wikitable2=wikitable2,
            wikitable3=wikitable3,
            wikitable4=wikitable4,
            wikitable5=wikitable5,
            types=missing_message(pagedeleted_missing.get('types', [])),
            occupations=missing_message(pagedeleted_missing.get('occupations', [])),
            backlinks=missing_message(pagedeleted_missing.get('backlinks', [])),
            identifiers=missing_message(pagedeleted_missing.get('identifiers', [])),
            properties=missing_message(pagedeleted_missing.get('properties', [])),
            update=strftime('%Y-%m-%d, %H:%I')
        )
    )


if __name__=='__main__':
    main()
