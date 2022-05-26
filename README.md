# Django webapp for text and product monitoring

Piece backend code of my monitoring text and product webapp.

Full working app https://robie-seo.herokuapp.com/crawl/

If you want to see the full code of the application, you can write to me

File review

## crawl_saas_custom.py

Main app file, tasks:
1. Extract the data input on the frontend from the database
2. If the frontend has not been given a list of url addresses, it filters and collects them from the sitemap and save to db
4. Visits each address on the list individually and get text, text length, amount of products, products name, H1 
5. Save all to db

## kwt_api_search_volumen.py
1. On H1 base gets search volume from KWT API
2. Save search volume to db

## save_to_sheet.py
1. Extracts data placed during crawl and kwt api from the database
2. Save all data to Google Sheets
3. Return Google Sheets URL

## Sample results sheet
https://docs.google.com/spreadsheets/d/1xj9sel6HNAgaWrQNQLODKNg05vRKv4c05NzMQ1ofwTc/
