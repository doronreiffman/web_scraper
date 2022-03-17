import metacritic_scraper as scrape
import pytest
import pandas as pd
import config as cfg


# ---------------  save_csv  --------------- #

def test_save_csv_exception_params_not_exists():
    with pytest.raises(TypeError):
        scrape.save_csv()


def test_save_csv_exception_dataframe_not_exists():
    args = scrape.parse_args(['year'])

    with pytest.raises(TypeError):
        scrape.save_csv(args)


def test_save_csv_exception_too_many_arguments():
    args = scrape.parse_args(['year'])

    with pytest.raises(TypeError):
        scrape.save_csv(args, pd.DataFrame(), pd.DataFrame())


def test_save_csv_exception_argument_wrong_type():
    args = scrape.parse_args(['year'])

    with pytest.raises(AttributeError):
        scrape.save_csv(args, [1, 2, 3])
    with pytest.raises(AttributeError):
        scrape.save_csv(args, 'a')


def test_save_csv_creates_or_modifies_file():
    args = scrape.parse_args(['-sthis', 'is', '-ytest'])

    scrape.save_csv(args, pd.DataFrame({'a': [1, 2]}))


def test_save_csv_empty_dataframe():
    args = scrape.parse_args(['-sthis', 'is', '-ytest'])

    scrape.save_csv(args, pd.DataFrame())


# ---------------  use_grequests  --------------- #

def test_use_grequests_exception_param_not_exists():
    with pytest.raises(TypeError):
        scrape.use_grequests()


def test_use_grequests_exception_too_many_arguments():
    test_url = ["https://www.metacritic.com"]

    with pytest.raises(TypeError):
        scrape.use_grequests(test_url, test_url)


def test_use_grequests_exception_argument_wrong_type():
    test_url = "https://www.metacritic.com"
    with pytest.raises(AttributeError):
        scrape.use_grequests(test_url)
    with pytest.raises(TypeError):
        scrape.use_grequests(1)


def test_use_grequests_exception_bad_url_link():
    test_url = ["https://www.metacritic.com/bad_link_for_testing"]
    with pytest.raises(ValueError):
        scrape.use_grequests(test_url)


def test_use_grequests_works_with_good_link():
    test_url = ["https://www.metacritic.com"]
    scrape.use_grequests(test_url)


# ---------------  scrape_album_page  --------------- #

def test_scrape_album_page_exception_params_not_exists():
    with pytest.raises(TypeError):
        scrape.scrape_album_page()


def test_scrape_album_page_exception_pages_url_not_exists():
    args = scrape.parse_args(['year'])

    with pytest.raises(TypeError):
        scrape.scrape_album_page(args)


def test_scrape_album_page_exception_too_many_arguments():
    args = scrape.parse_args(['year'])
    pages_url = cfg.TEST_PAGES

    with pytest.raises(TypeError):
        scrape.scrape_album_page(args, pages_url, pages_url)


def test_scrape_album_page_exception_argument_wrong_type():
    args = scrape.parse_args(['year'])
    pages_url1 = set(cfg.TEST_PAGES)
    pages_url2 = {n: url for n, url in enumerate(cfg.TEST_PAGES)}
    pages_url3 = 1

    with pytest.raises(TypeError):
        scrape.scrape_album_page(args, pages_url1)
    with pytest.raises(TypeError):
        scrape.scrape_album_page(args, pages_url2)
    with pytest.raises(TypeError):
        scrape.scrape_album_page(args, pages_url3)


def test_scrape_album_page_exception_bad_url_link():
    args = scrape.parse_args(['year'])
    pages_url = ["https://www.metacritic.com/bad_link_for_testing"]
    with pytest.raises(ValueError):
        scrape.scrape_album_page(args, pages_url)


def test_scrape_album_page_batch_bigger_than_pages_url_length():
    args = scrape.parse_args(['year', '-b1000'])
    pages_url = cfg.TEST_PAGES

    assert list(scrape.scrape_album_page(args, pages_url).keys()) == cfg.ALBUM_PAGE_COLUMNS


def test_scrape_album_page_exception_batch_negative_or_zero():
    pages_url = cfg.TEST_PAGES

    args = scrape.parse_args(['year', '-b-10'])
    with pytest.raises(ValueError):
        scrape.scrape_album_page(args, pages_url)

    args = scrape.parse_args(['year', '-b0'])
    with pytest.raises(ValueError):
        scrape.scrape_album_page(args, pages_url)


def test_scrape_album_page_empty_pages_url():
    args = scrape.parse_args(['year'])
    pages_url = []

    assert list(scrape.scrape_album_page(args, pages_url).keys()) == []


def test_scrape_album_page_creates_dictionary():
    args = scrape.parse_args(['year'])
    pages_url = [cfg.TEST_PAGES[0]]

    assert list(scrape.scrape_album_page(args, pages_url).keys()) == cfg.ALBUM_PAGE_COLUMNS


def test_scrape_album_page_exception_not_album_page():
    args = scrape.parse_args(['year'])
    chart_url = 'https://www.metacritic.com/'

    with pytest.raises(AttributeError):
        scrape.scrape_album_page(args, chart_url)


# ---------------  scrape_chart_page  --------------- #

def test_scrape_chart_page_exception_params_not_exists():
    with pytest.raises(TypeError):
        scrape.scrape_chart_page()


def test_scrape_chart_page_exception_pages_url_not_exists():
    args = scrape.parse_args(['year'])

    with pytest.raises(TypeError):
        scrape.scrape_chart_page(args)


def test_scrape_chart_page_exception_too_many_arguments():
    args = scrape.parse_args(['year'])
    chart_url = cfg.TEST_PAGES[0]

    with pytest.raises(TypeError):
        scrape.scrape_chart_page(args, chart_url, chart_url)


def test_scrape_chart_page_exception_argument_wrong_type():
    args = scrape.parse_args(['year'])
    chart_url1 = set(cfg.TEST_PAGES[0])
    chart_url2 = 1

    with pytest.raises(AttributeError):
        scrape.scrape_chart_page(args, chart_url1)
    with pytest.raises(AttributeError):
        scrape.scrape_chart_page(args, chart_url2)


def test_scrape_chart_page_exception_bad_url_link():
    args = scrape.parse_args(['year'])
    chart_url = "https://www.metacritic.com/bad_link_for_testing"
    with pytest.raises(ValueError):
        scrape.scrape_chart_page(args, chart_url)


def test_scrape_chart_page_exception_empty_chart_url():
    args = scrape.parse_args(['year'])
    chart_url = ''
    with pytest.raises(AttributeError):
        scrape.scrape_chart_page(args, chart_url)


def test_scrape_chart_page_max_bigger_than_chart_length():
    args = scrape.parse_args(['year', '-m1000'])
    chart_url = 'https://www.metacritic.com/browse/albums/score/metascore/year/filtered?year_selected=2021'

    assert list(scrape.scrape_chart_page(args, chart_url).keys()) == cfg.CHART_PAGE_COLUMNS


def test_scrape_chart_page_exception_batch_negative():
    chart_url = 'https://www.metacritic.com/browse/albums/score/metascore/year/filtered?year_selected=2021'

    args = scrape.parse_args(['year', '-m-10'])
    with pytest.raises(ValueError):
        scrape.scrape_chart_page(args, chart_url)

    args = scrape.parse_args(['year', '-m0'])
    with pytest.raises(ValueError):
        scrape.scrape_chart_page(args, chart_url)


def test_scrape_chart_page_creates_dictionary():
    args = scrape.parse_args(['year'])
    chart_url = 'https://www.metacritic.com/browse/albums/score/metascore/year/filtered?year_selected=2021'

    assert list(scrape.scrape_chart_page(args, chart_url).keys()) == cfg.CHART_PAGE_COLUMNS


def test_scrape_chart_page_exception_not_chart_page():
    args = scrape.parse_args(['year'])
    chart_url = 'https://www.metacritic.com/'

    with pytest.raises(AttributeError):
        scrape.scrape_chart_page(args, chart_url)
