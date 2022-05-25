from __future__ import print_function

from ..models import URL, Domain
import gspread
from datetime import datetime
from .kwt_api_search_volumen import KwtApi


def save_db_to_sheets(token: str) -> str:

    # get search volume to H1 (optional module)
    KwtApi(token=token).get_kw_list()

    credentials = "<YOUR CREDENTIALS>"

    gc = gspread.service_account_from_dict(credentials)

    # create new google sheets
    sh = gc.create(f'{Domain.objects.get(token=token).domain_adress} - znaki - {datetime.now()}')

    # share new google sheets to anyone as writer
    sh.share(value=None, perm_type="anyone", role="writer")

    # choice sheet1
    rows = sh.sheet1

    url_db = URL.objects.filter(token=token)
    url_list_len = len(url_db)

    # specify save range in google sheets
    cell_listA = rows.range(f'A1:A{url_list_len+1}')
    cell_listB = rows.range(f'B1:B{url_list_len+1}')
    cell_listC = rows.range(f'C1:C{url_list_len+1}')
    cell_listD = rows.range(f'D1:D{url_list_len+1}')
    cell_listE = rows.range(f'E1:E{url_list_len+1}')
    cell_listF = rows.range(f'F1:F{url_list_len+1}')

    cell_number = 0
    # name the first row in each column
    cell_listA[cell_number].value = "URL"
    cell_listB[cell_number].value = "ILOŚĆ ZNAKÓW"
    cell_listC[cell_number].value = "ILOŚĆ PRODUKTÓW"
    cell_listD[cell_number].value = "H1/FRAZA"
    cell_listE[cell_number].value = "ILOŚĆ WYSZUKAŃ"
    cell_listF[cell_number].value = "STATUS ODPOWIEDZI"
    rows.format('A1:F1', {'textFormat': {'bold': True}})
    cell_number = 1

    # save data from db to google sheets
    for i in range(url_list_len):
        cell_listA[cell_number].value = url_db[i].crawled_url
        try:
            cell_listB[cell_number].value = url_db[i].text_length
        except:
            cell_listB[cell_number].value = "Błąd bazy danych"
        try:
            cell_listC[cell_number].value = url_db[i].amount_of_products
        except:
            cell_listC[cell_number].value = "Błąd bazy danych"
        try:
            if not url_db[i].h1:
                raise EOFError
            cell_listD[cell_number].value = url_db[i].h1
        except:
            cell_listD[cell_number].value = "Błąd/Brak H1"
        try:
            if not url_db[i].h1_search_volumen:
                raise EOFError
            cell_listE[cell_number].value = url_db[i].h1_search_volumen
        except:
            cell_listE[cell_number].value = "Błąd KWT"
        try:
            cell_listF[cell_number].value = url_db[i].status_code
        except:
            cell_listF[cell_number].value = 500
        cell_number += 1

    # push to google sheets
    rows.update_cells(cell_listA)
    rows.update_cells(cell_listB)
    rows.update_cells(cell_listC)
    rows.update_cells(cell_listD)
    rows.update_cells(cell_listE)
    rows.update_cells(cell_listF)

    # get google sheets url
    google_sheets_id = sh.url

    # save url to db
    Domain.objects.filter(token=token).update(google_sheet_id=google_sheets_id)
    return google_sheets_id
