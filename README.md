# project1

## main takeaways and improvement from last time
1. applied OOP design principles by placing separate pieces of code into different classes
2. debugging:
    1. run unit test for each class's member functions by checking its output
    2. used pycharm debugger to check the lines of code that caused the error
    3. checked the value of some member variables that store the intermediate results
3. optimized the database cache management by adopting the "cache write-through" method: keep the database cache continuously updated and running independently. Users can query data from the database cache and immediately get the most updated data. 
 
## code design
### server.py
It has different classes for different tasks:
1. class "database": fetch historical data from source 1 and continuously fetch real-time data from source 2
2. class "trading_strategy": calculate the trading strategy analystics like signal time series, pnl time series, etc
3. class "report": generate the final report
4. class "controller": coordinate among database, trading_strategy and report. Control these three classes in one
----
5. class "communication

