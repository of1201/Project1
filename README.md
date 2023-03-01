# project1

## main takeaways and improvement from last time
1. applied OOP design principles by decoupling logically separate pieces
2. debugging:
    1. run unit test for each class's member functions by checking its output
    2. used pycharm debugger to check the lines of code that caused the error
    3. checked the value of some member variables that store the intermediate results
3. optimized the database cache management by adopting the "cache write-through" method: keep the database cache continuously updated and running independently. Users can query data from the database cache and immediately get the most updated data. 
4. enabled multi-threading (and locking) for database updating and multi-client connections establishing. This was implemented in last version already.
 
## code design
### server.py
It has different classes for different tasks:
1. class "database": fetch historical data from source 1 and continuously fetch real-time data from source 2
2. class "trading_strategy": calculate the trading strategy analystics like signal time series, pnl time series, etc
3. class "genReport": generate the final report
4. class "controller": coordinate among database, trading_strategy and report. Control these three classes in one
5. class "server_parser": parse server arguments 
6. class "client_parser": parse client inputs and send server results back to clients
7. class "communication": coordinate among controller, server_parser and client_parser. It parse server arguments and client inputs first, then ask the server to perform tasks according to the inputs, and finally send the results back to clients.
### client.py
1. class "communication": parse client inputs, send to server and receive results from server
