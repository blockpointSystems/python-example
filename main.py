# This is a sample Python script.
from mdb_bp import driver
from datetime import datetime
import csv

databaseName = "main"
productBlockchainName = "product"
materialsBlockchainName = "material"
projectBlockchainName = "project"

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Connect to the database
    conn = driver.connect(
        username="system",
        password="biglove",
        connection_protocol="tcp",
        server_address="localhost",
        server_port=5461,
        database_name="master",
        parameters={"interpolate_params": True},
    )

    # Check for an existing database
    dbInitialized = False
    rows = conn.query("SELECT name FROM sys_database WHERE sys_database.name = ?", [databaseName])
    itr = iter(rows)

    for row in itr:
        dbInitialized = True

    if not dbInitialized:
        result = conn.prepare(
            "CREATE DATABASE %s" % databaseName).exec()
        print(result)

    result = conn.prepare(
        "USE %s" % databaseName).exec()
    print(result)

    # Check for an existing blockchain
    productsInitialized = False
    rows = conn.query("SELECT sys_blockchain_id FROM sys_blockchain WHERE sys_blockchain.name = ?",
                      [productBlockchainName])
    itr = iter(rows)
    for row in itr:
        productsInitialized = True

    if not productsInitialized:
        conn.prepare(
            "CREATE BLOCKCHAIN %s.%s TRADITIONAL " % (databaseName, productBlockchainName) +
            "(product_id UINT64 PRIMARY KEY AUTO INCREMENT," +
            " product_name STRING SIZE = 25 PACKED," +
            " price_per_square_foot FLOAT32)"
        ).exec()

    # Check for an existing blockchain
    projectsInitialized = False
    rows = conn.query("SELECT sys_blockchain_id FROM sys_blockchain WHERE sys_blockchain.name = ?",
                      [projectBlockchainName])
    itr = iter(rows)
    for row in itr:
        projectsInitialized = True

    if not projectsInitialized:
        conn.prepare(
            "CREATE BLOCKCHAIN %s.%s TRADITIONAL " % (databaseName, projectBlockchainName) +
            "(project_id UINT64 PRIMARY KEY AUTO INCREMENT," +
            " project_name STRING SIZE = 25 PACKED," +
            " project_location UINT64)"
        ).exec()

    # Check for an existing blockchain
    materialsInitialized = False
    rows = conn.query("SELECT sys_blockchain_id FROM sys_blockchain WHERE sys_blockchain.name = ?",
                      [materialsBlockchainName])
    itr = iter(rows)
    for row in itr:
        materialsInitialized = True

    if not materialsInitialized:
        conn.prepare(
            "CREATE BLOCKCHAIN %s.%s TRADITIONAL " % (databaseName, materialsBlockchainName) +
            "(material_id UINT64 PRIMARY KEY AUTO INCREMENT," +
            " project_id UINT64 FOREIGN [main.project, project_id]," +
            " product_id UINT64 FOREIGN [main.product, product_id]," +
            " volume FLOAT32)"
        ).exec()

    # Add products using the csv file
    steelProductId = 0
    products = csv.reader(open('files/products.csv'),  delimiter=',')
    for product in products:
        res = conn.prepare(
            "INSERT %s.%s (product_name, price_per_square_foot) VALUES " % (databaseName, productBlockchainName) +
            "(?, ?)"
        ).exec([product[0], product[1]])
        if product[0] == "steel":
            steelProductId = res.insert_id

    # Add project
    res = conn.prepare(
        "INSERT %s.%s (project_name, project_location) VALUES (?, ?) " %
        (databaseName, projectBlockchainName)
    ).exec(["San Diego", 92131])

    # Add materials
    conn.prepare(
        "INSERT %s.%s (project_id, product_id, volume) VALUES (?, ?, 100) " %
        (databaseName, materialsBlockchainName)
    ).exec([res.insert_id, steelProductId])

    # Get the total cost of all projects (python doesn't support FLOAT64, must cast to FLOAT32 before returning)
    rows = conn.query("SELECT project.project_id, (SUM(material.volume * product.price_per_square_foot)) " +
                      "FROM project JOIN material" +
                      " ON project.project_id = material.project_id " +
                      " JOIN product ON product.product_id = material.product_id " +
                      " GROUP BY project.project_id")
    itr = iter(rows)
    for row in itr:
        print(row)

    # Change the price of steel
    steelProductId = conn.prepare(
        "AMEND %s.%s (product_id, product_name, price_per_square_foot) VALUES " % (databaseName, productBlockchainName) +
        "(?, ?, ?)"
    ).exec([steelProductId, "steel", 12.0]).insert_id

    # Get the new cost of the project
    rows = conn.query("SELECT *, STRING(price_per_square_foot), sys_timestamp FROM product")
    itr = iter(rows)
    for row in itr:
        print(row)
