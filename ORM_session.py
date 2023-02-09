import sqlalchemy
from ORM_models import create_tables, Publisher, Book, Stock, Shop, Sale
from sqlalchemy.orm import sessionmaker
import json
import os
import settings

DSN = f'postgresql://{os.getenv(settings.login)}:{os.getenv(settings.password)}@localhost:5432/' \
      f'{os.getenv(settings.db_name)}'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)
Session = sessionmaker(bind=engine)
session = Session()
with open('fixtures.json', 'r') as data_db:
    data = json.load(data_db)
for model in data:
    if model['model'] == 'publisher':
        publisher = Publisher(id=model['pk'], name=model['fields']['name'])
        session.add(publisher)
    if model['model'] == 'book':
        book = Book(id=model['pk'], title=model['fields']['title'], publisher_id=model['fields']['publisher_id'])
        session.add(book)
    if model['model'] == 'shop':
        shop = Shop(id=model['pk'], name=model['fields']['name'])
        session.add(shop)
    if model['model'] == 'stock':
        stock = Stock(id=model['pk'], book_id=model['fields']['book_id'], shop_id=model['fields']['shop_id'],
                      count=model['fields']['count'])
        session.add(stock)
    if model['model'] == 'sale':
        sale = Sale(id=model['pk'], price=model['fields']['price'], date_sale=model['fields']['date_sale'],
                    stock_id=model['fields']['stock_id'], count=model['fields']['count'])
        session.add(sale)
    session.commit()


def book_data(session_, publisher_name):
    for book_, shop_, price, date_sale in session_.query(Book.title, Shop.name, Sale.price, Sale.date_sale)\
            .join(Publisher.book).join(Book.stock).join(Stock.sale).join(Stock.shop)\
            .filter(Publisher.name == publisher_name):
        print(book_, "|", shop_, "|", price, "|", date_sale)


book_data(session, input())