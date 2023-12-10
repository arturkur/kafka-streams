# Running the app

- `cd devcontainer`
- `docker compose up -d`
- Wait until services are up (http://localhost:8083/connectors responds)
- `./init_connectors.sh`
- Run `PurchaseOrdersApp.main()` and `CustomersApp.main()`
- Insert some data to supplier table
  - Example how to do it:
    - `docker exec -it database /bin/bash`
    - `psql -U postgres -d sock_shop`
    - `insert into supplier values (gen_random_uuid(), 'test 1');`
  - this triggers the generation of socks, purchases and sales
  - you have to insert at least one supplier before starting Kafka Streams, because otherwise the sale topic isn't created
- Run `KafkaStreamsApp.main()`
- All results are created to `result` table
  - You can find specific requirement results by result id
    - `select * from result where id like 'req11%';`

## Requirements

1. Add sock suppliers to the database. To simplify sock suppliers cannot be deleted
   and optionally not changed.
2. List sock suppliers from	the	database.
3. Add socks to	be purchased to	the	database. Again, these cannot be deleted, but
   may be changed if students wish.
4. List socks from the database.
5. Get the revenue	per	sock pair sale.
6. Get the expenses per sock pair sale.
7. Get the profit per sock pair sale.
8. Get the total revenues.
9. Get the total expenses.
10. Get	the	total profit.
11. Get	the	average	amount spent in each purchase (separated by sock type).
12. Get	the	average	amount spent in each purchase (aggregated for all socks).
13. Get	the	sock type with the highest	profit of all (only one if there is a tie).
14. Get	the	total revenue in the last hour1 (use a tumbling time window).
15. Get	the	total expenses in the last hour (use a tumbling time window).
16. Get	the	total profits	in the last hour (use a tumbling time window).
17. Get	the	name of the sock supplier generating the highest profit sales. Include the value	of such sales.