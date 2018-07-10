
from collections import namedtuple
import datetime as dt
from functools import reduce
import itertools as it
import re
import sqlite3
from urllib.parse import parse_qs, urljoin, urlparse, quote as urlquote

from selenium.common.exceptions import WebDriverException
from splinter import Browser

base_url = 'http://www.ina.gl/inatsisartut/sammensaetning-af-inatsisartut/'

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
    '2014-11-28': '12',
    '2018-04-24': '13',}

appt_dates_to_terms = {
    **election_dates_to_terms,
    '1979-05-01': '1',
    '2015-11-03': '12',
    '2016-05-23': '12',
    '2016-09-16': '12',
    '2016-10-31': '12',
    '2017-01-30': '12',
    '2017-02-23': '12',
    '2017-04-24': '12',
    '2017-09-20': '12',
    '2017-09-22': '12',
    '2017-10-09': '12',
    '2018-01-11': '12',
    '2018-05-15': '13',}


def shift_date(date, **delta_kwargs):
    if date:
        return (dt.datetime.strptime(date, '%Y-%m-%d').date() +
                dt.timedelta(**delta_kwargs)).isoformat()


def extract_name(name):
    # element.text doesn't work on hidden elements apparently
    name = name._element.get_attribute('textContent')
    new_name, *_ = name.partition(',')
    if name != new_name:
        print(f'{name!r} converted to {new_name!r}')
    return ' '.join(new_name.split())


def extract_group(group):
    # A kludge to work around not being able to operate on text nodes
    # in Selenium
    group = group._element.get_attribute('innerText').splitlines()[2]
    return group.replace('_', ' '), group.replace(' ', '_').lower()


def extract_photo(photo):
    photo, = parse_qs(urlparse(photo).query)['image']
    if 'INAT-dukke-lys.jpg' in photo:
        return None
    return urljoin(base_url, urlquote(photo))


t12_appt_dates = sorted(k for k, v in appt_dates_to_terms.items() if v == '12')
t12_appt_dates = [(p, shift_date(n, days=-1) or '')
                  for p, n in zip(t12_appt_dates,
                                  t12_appt_dates[1:] + [None])]

def extract_appt_dates(term, option_date):
    if term != '12':
        return ('', '')
    return next(filter(lambda i: i[0] == option_date, t12_appt_dates))


_Row = namedtuple('Person', 'name, email, image, term, group, group_id, '
                            'start_date, end_date')

def scrape_rows(session, option_date):
    term = appt_dates_to_terms[option_date]
    for row in session.find_by_xpath(
            '//div[@id="cvtabbarmain"]/div[not(contains(@class, "ui-tabs-hide"))]'
            '//tr[starts-with(@id, "rowDetail")]/td/div[1]'):
        yield _Row(extract_name(row.find_by_xpath('./div/strong').first),
                   (row.find_by_xpath('.//a[starts-with(@href, "mailto")]')['href']
                       .replace('mailto:', '') or None),
                   extract_photo(row.find_by_xpath('./img')['src']),
                   term,
                   *extract_group(row.find_by_xpath('./div').first),
                   *extract_appt_dates(term, option_date))


def gather_people(session):
    options = [i['value'] for i in session.find_by_xpath('//*[@id = "valgdatoer"]/option')]
    options = sorted(set(options), key=options.index)
    for option, option_date in ((o, '-'.join(o.split('-')[::-1]))
                                for o in options):
        session.find_option_by_value(option).click()
        while 'display: none' not in session.find_by_id('loader').outer_html:
            ...
        yield from scrape_rows(session, option_date)
        on_leave = session.find_by_xpath('//a[text() = "Sulinngiffeqarpoq"]')
        if on_leave:
            try:
                on_leave.click()
            except WebDriverException:
                pass
            yield from scrape_rows(session, option_date)


def transform_groups(seq, val):
    a, b = val
    if not a or shift_date(a.end_date, days=1) == b.start_date:
        return [*seq[:-1], seq[-1] + [val[1]]]
    else:
        return seq + [[val[1]]]


def merge_date_adjacent_appts(appts):
    groups = reduce(transform_groups,
                    zip([None] + appts, appts),
                    [[]])
    for rows in groups:
        if len(rows) > 1:
            yield (*rows[0][:-1], rows[-1][-1])
        else:
            yield rows[0]


def main():
    with Browser('chrome', headless=True) as browser:
        browser.visit(base_url)
        people = list(gather_people(browser))
    with sqlite3.connect('data.sqlite') as c:
        c.execute('''\
CREATE TABLE IF NOT EXISTS data
(name, email, image, term, 'group', group_id, start_date, end_date,
 UNIQUE (name, term, 'group', start_date, end_date))''')
        c.executemany('''\
INSERT OR REPLACE INTO data VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            it.chain((i
                      for _, v in it.groupby(sorted({i for i in people if i.term == '12'}),
                                             key=lambda i: (i.name, i.group))
                      for i in merge_date_adjacent_appts(list(v))),
                     (i for i in people if i.term != '12')))
        c.execute('''\
CREATE TABLE IF NOT EXISTS terms
(id, name, start_date, end_date, UNIQUE (id))''')
        c.executemany('''\
INSERT OR REPLACE INTO terms VALUES (?, ?, ?, ?)''',
            ((election_dates_to_terms[s],
              # http://en.inatsisartut.gl/media/1595595/om_inatsisartut_vers_2014.01_en_web.pdf, p. 7
              ('Landsting' if int(s[:4]) < 2009 else 'Inatsisartut' + ' ' +
               election_dates_to_terms[s]),
              s,
              shift_date(e, days=-1))
             for s, e in zip(sorted(election_dates_to_terms),
                             sorted(election_dates_to_terms)[1:] + [None])))

if __name__ == '__main__':
    main()
