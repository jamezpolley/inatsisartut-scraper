
import itertools as it
from operator import itemgetter
import sqlite3
import sys

import lxml.html
from splinter import Browser

base_url = 'http://www.ina.gl/inatsisartuthome/sammensaetning-af-inatsisartut.aspx'

# http://www.ina.gl/media/28274/Valg%20til%20Inatsisartut%20DA%20WEB.pdf, pp. 31-33
# http://lovgivning.gl/Lov?rid=%7b83F511C8-78BE-4B26-8277-291DFE01D57E%7d&sc_lang=da-DK
election_dates_to_terms = {
    '1979-04-04': '1',
    '1983-04-12': '2',
    '1984-06-06': '3',
    '1987-05-26': '4',
    '1991-03-05': '5',
    '1995-03-04': '6',
    '1999-02-16': '7',
    '2002-12-03': '8',
    '2005-11-15': '9',
    '2009-06-02': '10',
    '2013-03-12': '11',
    '2014-11-28': '12',}

session_dates_to_terms = {'-'.join(k.split('-')[::-1]): v for k, v in {
    **election_dates_to_terms,
    '1979-05-01': '1',
    '2015-11-03': '12',
    '2016-05-23': '12',}.items()}


def extact_name(name):
    name, = name
    new_name, *_ = name.partition(',')
    if name != new_name:
        print('=> {!r} converted to {!r}'.format(name, new_name),
              file=sys.stderr)
    return new_name


def parse_html(html):
    doc = lxml.html.document_fromstring(html)
    doc.make_links_absolute(base_url)
    return doc.xpath('body')[0]


def scrape_row(row, term):
    html = parse_html(row.html)
    return (extact_name(html.xpath('div/strong/text()')),
            (html.xpath('//a[starts-with(@href, "mailto")]/@href')[0]
             .replace('mailto:', '') or None),
            html.xpath('img/@src')[0],
            term,
            html.xpath('div/text()[2]')[0].replace('_', ' '))


def gather_people(session):
    options = parse_html(session.find_by_id('valgdatoer').html)\
        .xpath('//option/@value')
    options = sorted(set(options), key=options.index)
    options.remove('28-11-2014')   # Won't load
    for option in options:
        session.find_option_by_value(option).click()
        while 'display: none' not in session.find_by_id('loader').outer_html:
            ...
        yield tuple(scrape_row(r, session_dates_to_terms[option])
                    for r in session.find_by_xpath('//div[@id="cvtabbarmain"]/div[last()-1]'
                                                   '//tr[starts-with(@id, "rowDetail")]/td/div[1]'))


def main():
    with Browser('phantomjs', load_images=False) as browser:
        browser.visit(base_url)
        people = tuple(it.chain.from_iterable(gather_people(browser)))
    with sqlite3.connect('data.sqlite') as c:
        c.execute('''\
CREATE TABLE IF NOT EXISTS data
(name, email, image, term, 'group', UNIQUE (name, term, 'group'))''')
        c.executemany('''\
INSERT OR REPLACE INTO data VALUES (?, ?, ?, ?, ?)''', people)
        c.execute('''\
CREATE TABLE IF NOT EXISTS terms
(id, name, start_date, end_date, UNIQUE (id))''')
        c.executemany('''\
INSERT OR REPLACE INTO terms VALUES (?, ?, ?, ?)''',
            ((v, v, k, None) for k, v in
             sorted(election_dates_to_terms.items(), key=itemgetter(1))))

if __name__ == '__main__':
    main()
