import os
import sqlite3


class Hat:
    def __init__(self, my_id, topping, supplier, quantity):
        self.id = my_id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, my_id, name):
        self.id = my_id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
        INSERT INTO suppliers (id, name) VALUES (?, ?)  
        """, [supplier.id, supplier.name])

    def find_supplier(self, my_id):
        c = self._conn.cursor()
        c.execute("""
        SELECT name FROM suppliers WHERE id = ?
        """, [my_id])
        return c.fetchone()[0]


class _Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, order):
        self._conn.execute("""
            INSERT INTO orders (id, location, hat) VALUES (?, ?, ?)  
        """, [order.id, order.location, order.hat])


class _Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("""
            INSERT INTO hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)  
        """, [hat.id, hat.topping, hat.supplier, hat.quantity])

    def deleteline(self, hatid):
        self._conn.execute("""
        DELETE FROM hats WHERE id = ?
        """, [hatid])

    def decreasequantity(self, hatquantity, hatid):
        self._conn.execute("""
        UPDATE hats SET quantity = ? WHERE id = ?
        """, [hatquantity, hatid])

    def find_id_quantity_supplier(self, mytopping, my_supplier):
        c = self._conn.cursor()
        c.execute("""
        SELECT id,quantity FROM hats WHERE topping = ? and supplier = ?
        """, [mytopping, my_supplier])
        return c.fetchone()

    def find_ids(self, mytopping):
        c = self._conn.cursor()
        c.execute("""
        SELECT supplier FROM hats WHERE topping = ?
        """, [mytopping])
        return c.fetchall()



class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.hats = _Hats(self._conn)
        self.orders = _Orders(self._conn)
        self.suppliers = _Suppliers(self._conn)

    def close(self):
        self._conn.commit()
        # self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE hats (
            id        INT     PRIMARY KEY,
            topping   TEXT    NOT NULL,
            supplier  INT     NOT NULL,
            quantity  INT     NOT NULL,
            
            FOREIGN KEY(supplier) REFERENCES suppliers(id)
        ); 
        CREATE TABLE suppliers (
            id     INT       PRIMARY KEY,
            name   TEXT      NOT NULL
        ); 
        CREATE TABLE orders (
            id         INT     PRIMARY KEY,
            location   TEXT    NOT NULL,
            hat        INT     NOT NULL,
            
            FOREIGN KEY(hat) REFERENCES hats(id)
        ); 

    """)


def main():

    output = open("output.txt", "w+")
    # remove DB before creating the a new one
    os.remove("database.db")
    repo = _Repository()
    repo.create_tables()
    orders_text = open("orders.txt", "r")
    config_text = open("config.txt", "r")
    config_size = config_text.readline().split(',')

    # Put all the config file in array
    temp = [line for line in config_text]
    temp[len(temp)-1] = temp[len(temp)-1] + "\n"
    config_array = [line[:-1] for line in temp]

    # slice the array into suppliers and hats lists
    suppliers_list = config_array[int(config_size[0]):]
    hats_list = config_array[:int(config_size[0])]

    # Insert the suppliers to the DB
    for i in range(int(config_size[1])):
        sup = suppliers_list[i].split(',')
        repo.suppliers.insert(Supplier(sup[0], sup[1]))

    # Insert the hats to the DB
    for i in range(int(config_size[0])):
        hat = hats_list[i].split(',')
        repo.hats.insert(Hat(hat[0], hat[1], hat[2], hat[3]))

    # Put all the order into array
    temp = [line for line in orders_text]
    temp[len(temp)-1] = temp[len(temp)-1] + "\n"
    order_array = [line[:-1] for line in temp]

    # Insert all the order into the DB
    for i in range(len(temp)):
        order = order_array[i].split(',')
        # find the first row match the topping and receive the id, quantity
        suppliers_list = sorted(repo.hats.find_ids(order[1]))
        hat_supplier_id = suppliers_list[0][0]
        id_quantity = repo.hats.find_id_quantity_supplier(order[1], hat_supplier_id)
        hat_id = id_quantity[0]
        hat_quantity = id_quantity[1]
        # Decrease the quantity of the specific id row
        repo.hats.decreasequantity(hat_quantity-1, hat_id)
        # Remove the row if the quantity is 0 after the decrease
        if hat_quantity == 1:
            repo.hats.deleteline(hat_id)
        # Insert the order in the 'orders' table in the DB
        repo.orders.insert(Order(i+1, order[0], hat_id))
        # Insert row into the output file
        hat_supplier = repo.suppliers.find_supplier(hat_supplier_id)
        output.writelines(order[1] + "," + hat_supplier + "," + order[0] + "\n")
        repo.close()


if __name__ == '__main__':
    main()
