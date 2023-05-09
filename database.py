from sqlalchemy import create_engine, event, MetaData, String, ForeignKey
from sqlalchemy.sql.expression import select, func
from sqlalchemy.orm import Mapped, mapped_column, Session, DeclarativeBase, query
import datetime

engine = create_engine("postgresql+psycopg2://student_pnorov5:qwerty123@dc-webdev.bmstu.ru:8080/student_pnorov5")

@event.listens_for(engine, "connect", insert=True)
def set_search_path(dbapi_connection, connection_record):
    existing_autocommit = dbapi_connection.autocommit
    dbapi_connection.autocommit = True
    cursor = dbapi_connection.cursor()
    cursor.execute("SET SESSION search_path='%s'" % "test")
    cursor.close()
    dbapi_connection.autocommit = existing_autocommit

connection = engine.connect()
metadata = MetaData()

class Base(DeclarativeBase):
    type_annotation_map = {
        str: String(30)
    }


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    category: Mapped[str] = mapped_column()
    brand: Mapped[str] = mapped_column()

    def __repr__(self) -> str:
        return f"Product(id = {self.product_id}, name = {self.name}, category = {self.category}, brand = {self.brand})"

class Stores(Base):
    __tablename__ = "stores"

    store_id: Mapped[int] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column()
    region: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Stores(store_id = {self.store_id}, address = {self.address}, region = {self.region})"

class Customers(Base):
    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    surname: Mapped[str] = mapped_column()
    birth_date: Mapped[datetime.datetime] = mapped_column(nullable=False)

    def __repr__(self):
        return f"Customer(customer_id = {self.customer_id}, name = {self.name}, surname = {self.surname}, birth_date = {self.birth_date})"

class Prices(Base):
    __tablename__ = "prices"

    price_id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.product_id"))
    price: Mapped[int] = mapped_column()
    start_date: Mapped[datetime.datetime] = mapped_column(nullable=False)
    end_date: Mapped[datetime.datetime] = mapped_column(nullable=False)

'''with Session(engine) as session1:
    chair = Product(
        name="chair",
        category="furniture",
        brand="Art Life"
    )
    laptop = Product(
        name="laptop",
        category="technique",
        brand="Honor"
    )
    fridge = Product(
        name="fridge",
        category="technique",
        brand="LG"
    )
    milk = Product(
        name="milk",
        category="food",
        brand="EcoMilk"
    )
    trousers = Product(
        name="trousers",
        category="cloth",
        brand="Armani"
    )

    session1.add_all([chair, laptop, fridge, milk, trousers])
    session1.commit()

with Session(engine) as session2:
    lenta = Stores(
        address="Тихорецкий б-р 12к2",
        region="Msc"
    )
    mega = Stores(
        address="Котельники",
        region="Msc"
    )
    dns = Stores(
        address="Мурино",
        region="Spb"
    )

    session2.add_all([lenta, mega, dns])
    session2.commit()

with Session(engine) as session3:
    parviz = Customers(
        name="Parviz",
        surname="Norov",
        birth_date="2002-10-28"
    )
    umed = Customers(
        name="Umed",
        surname="Bazarov",
        birth_date="2002-03-05"
    )
    asirbek = Customers(
        name="Asirbek",
        surname="Umarboev",
        birth_date="2001-03-15"
    )

    session3.add_all([parviz, umed, asirbek])
    session3.commit()

with Session(engine) as session5:
    nurik = Customers(
        name="Nurik",
        surname="Mamedov",
        birth_date="2003-08-12"
    )
    amina = Customers(
        name="Amina",
        surname="Norova",
        birth_date="2028-01-03"
    )
    nigora = Customers(
        name="Nigora",
        surname="Norova",
        birth_date="2006-07-07"
    )
    muhammad = Customers(
        name="Muhammad",
        surname="Yorov",
        birth_date="1997-03-16"
    )
    jamshed = Customers(
        name="Jamshed",
        surname="Pirov",
        birth_date="2002-09-21"
    )

    session5.add_all([nurik, amina, nigora, muhammad, jamshed])
    session5.commit()

with Session(engine) as session4:
    price1 = Prices(
        product_id=1,
        price=123,
        start_date="2017-04-12",
        end_date="2018-09-13"
    )
    price2 = Prices(
        product_id=2,
        price=436,
        start_date="2016-02-11",
        end_date="2018-10-29"
    )
    price3 = Prices(
        product_id=2,
        price=3427,
        start_date="2010-01-01",
        end_date="2012-07-15"
    )
    price4 = Prices(
        product_id=3,
        price=586,
        start_date="2023-04-08",
        end_date="2023-05-07"
    )

    session4.add_all([price1, price2, price3, price4])
    session4.commit()'''

def get_5_random_customers():
    with Session(engine) as session:
        ans = []
        stmt = select(Customers).order_by(func.random()).limit(5)
        xs = list(session.scalars(stmt))
        for customer in xs:
            d = {}
            d["customer_id"] = customer.customer_id
            d["name"] = customer.name
            d["surname"] = customer.surname
            ans.append(d)
        return ans

def get_store_info(id):
    with Session(engine) as session:
        ans = {}
        stmt = select(Stores).where(Stores.store_id == id)
        xs = list(session.scalars(stmt))
        for info in xs:
            ans["address"] = info.address
            ans["region"] = info.region
        return ans

def get_max_price():
    with Session(engine) as session:
        ans = {}
        max_price = session.scalars(select(func.max(Prices.price))).first()
        stmt = select(Prices).where(Prices.price == max_price)
        for i in session.scalars(stmt):
            ans["product_id"] = i.product_id
            ans["price"] = i.price
            ans["start_date"] = i.start_date.strftime("%Y:%m:%d")
        ans["max_price"] = max_price
        return ans

def add_store_to_table():
    with Session(engine) as session:
        #cr7 = Stores(
            #address="Aviapark",
            #region="Dushanbe"
        #)
        #session.add(cr7)
        #session.commit()
        res = []
        stmt = session.query(Stores).all()
        for i in stmt:
            ans = {}
            ans["address"] = i.address
            ans["region"] = i.region
            res.append(ans)
        return res
#print(add_store_to_table())
Base.metadata.create_all(engine)





