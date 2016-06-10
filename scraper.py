
import datetime as dt
from operator import itemgetter
import re
import sqlite3
from urllib.parse import parse_qs, urljoin, urlparse

from lxml.html import document_fromstring as parse_html
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

term_end_dates = {s: e and (dt.datetime.strptime(e, '%Y-%m-%d').date() -
                            dt.timedelta(days=1)).isoformat()
                  for s, e in zip(sorted(election_dates_to_terms),
                                  sorted(election_dates_to_terms)[1:] + [None])}


def date_to_chamber(date, _date_match=re.compile(r'\d{4}')):
    # http://en.inatsisartut.gl/media/1595595/om_inatsisartut_vers_2014.01_en_web.pdf, p. 7
    year = int(_date_match.search(date).group())
    return 'Landsting' if year < 2009 else 'Inatsisartut'


def extract_name(name):
    name, = name
    new_name, *_ = name.partition(',')
    if name != new_name:
        print('=> {!r} converted to {!r}'.format(name, new_name))
    return new_name


def extract_group(group):
    group, = group
    return group.replace('_', ' '), group.replace(' ', '_').lower()


def extract_photo(photo):
    photo, = photo
    photo, = parse_qs(urlparse(photo).query)['image']
    if 'INAT-dukke-lys.jpg' in photo:
        return None
    return urljoin(base_url, photo)


def scrape_rows(doc, term, chamber):
    for row in doc.xpath(
            '//div[@id="cvtabbarmain"]/div[not(contains(@class, "ui-tabs-hide"))]'
            '//tr[starts-with(@id, "rowDetail")]/td/div[1]'):
        yield (extract_name(row.xpath('div/strong/text()')),
               (row.xpath('//a[starts-with(@href, "mailto")]/@href')[0]
                   .replace('mailto:', '') or None),
               extract_photo(row.xpath('img/@src')),
               term,
               *extract_group(row.xpath('div/text()[2]')),
               chamber)


def gather_people(session):
    options = parse_html(session.find_by_id('valgdatoer').html)\
        .xpath('//option/@value')
    options = sorted(set(options), key=options.index)
    options.remove('28-11-2014')   # Won't load
    for option in options:
        session.find_option_by_value(option).click()
        while 'display: none' not in session.find_by_id('loader').outer_html:
            ...
        term, chamber = session_dates_to_terms[option], date_to_chamber(option)
        yield from scrape_rows(parse_html(session.html), term, chamber)

        on_leave = session.find_by_xpath('//a[text() = "Sulinngiffeqarpoq"]')
        if on_leave:
            on_leave.click()
            yield from scrape_rows(parse_html(session.html), term, chamber)


def main():
    with Browser('phantomjs', load_images=False) as browser:
        browser.visit(base_url)
        people = tuple(gather_people(browser))
    with sqlite3.connect('data.sqlite') as c:
        c.execute('''\
CREATE TABLE IF NOT EXISTS data
(name, email, image, term, 'group', group_id, chamber,
 UNIQUE (name, term, 'group'))''')
        c.executemany('''\
INSERT OR REPLACE INTO data VALUES (?, ?, ?, ?, ?, ?, ?)''', people)
        c.execute('''\
CREATE TABLE IF NOT EXISTS terms
(id, name, start_date, end_date, UNIQUE (id))''')
        c.executemany('''\
INSERT OR REPLACE INTO terms VALUES (?, ?, ?, ?)''',
            ((v, date_to_chamber(k) + ' ' + v, k, term_end_dates[k])
             for k, v in sorted(election_dates_to_terms.items(),
                                key=itemgetter(1))))

if __name__ == '__main__':
    main()
