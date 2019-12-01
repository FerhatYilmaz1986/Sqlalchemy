from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pprint import pprint
from datetime import datetime

# Create an engine that stores data in the local directory's
# sqlalchemy_example.db file.
engine = create_engine('sqlite:////web/Sqlite-Data/example.db')

# this loads the sqlalchemy base class
Base = declarative_base()


# Setting up the classes that create the record objects and define the schema

class Person(Base):
    __tablename__ = 'person'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)


class Address(Base):
    __tablename__ = 'address'
    # Here we define columns for the table address.
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    street_name = Column(String(250))
    street_number = Column(String(250))
    post_code = Column(String(250), nullable=False)
    # creates the field to store the person id
    person_id = Column(Integer, ForeignKey('person.id'))
    # creates the relationship between the person and addresses.  backref adds a property to the Person class to retrieve addresses
    person = relationship("Person", backref="addresses")
class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer(), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    username = Column(String(50), nullable=False)
    email = Column(String(200), nullable=False)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)

Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
# A DBSession() instance establishes all conversations with the database
# and represents a "staging zone" for all the objects loaded into the
# database session object. Any change made against the objects in the
# session won't be persisted into the database until you call
# session.commit(). If you're not happy about the changes, you can
# revert all of them back to the last commit by calling
# session.rollback()
session = DBSession()
#Insert customers:
c1 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com')
session.add(c1)
c2 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com')
session.add(c2)
session.commit()
#Insert more customers
c3 = Customer(
    first_name="John",
    last_name="Lara",
    username="johnlara",
    email="johnlara@mail.com")

c4 = Customer(
    first_name="Sarah",
    last_name="Tomlin",
    username="sarahtomlin",
    email="sarahtomlin@mail.com")

c5 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com')

c6 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com')

session.add_all([c3, c4, c5, c6])
session.commit()
# Insert a Person in the person table
new_person1 = Person(name='Keith')
session.add(new_person1)

new_person2 = Person(name='Joe')
session.add(new_person1)

new_person3 = Person(name='Steve')
session.add(new_person1)
session.commit()

# Insert an Address in the address table using a loop

addresses = [
    Address(post_code='00001', person=new_person1),
    Address(post_code='00002', person=new_person2),
    Address(post_code='00003', person=new_person3),
]

# Loop through addresses and commit them to the database
for address in addresses:
    session.add(address)
    session.commit()

# joins Person on Address
all_people = session.query(Person).join(Address).all()

# Accessing a person with their address, You have to loop the addresses property and remember it was added by the
# backref on the addresses class
for person in all_people:
    # use the __dict__ magic method to have the object print it's properties
    pprint(person.__dict__)
    for address in person.addresses:
        pprint(address.__dict__)

# Retrieving the inverse of the relationship.  Notice I reverse the Person and Address to load the Address table
all_addresses = session.query(Address).join(Person).all()
for address in all_addresses:
    # showing how to use the print function with printing text and data at the same time easily
    print(f'{address.person.name} has a postal code of {address.post_code}')