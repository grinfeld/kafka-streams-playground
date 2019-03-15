Kafka-Streams and Windowing
--------------------

1. windowedBy operator with static window
    * events emitted immediately after received (no suppress)
    1. No TimeExtractor (window size: 2 minutes) **process time**
        * window is opened every 2 minutes. Actually, we don't have late arrival messages, so 
        messages entered inside 2 minute window
    1. With TimeExtractor (window size: 2 minutes) - **event time**
        1. sending message with time extracted older then processing time
            * opened window with 2 minute boundary and calculates aggregation (from starting)
        1. sending message with time extracted older then processing time and 
        inside the window boundaries, but at time (processing) less 
        then 2 minutes after window created
            * message inserted in the created window and 
            calculates aggregation (continues previous calculation)
        1. sending message with time extracted older then processing time and 
        inside the window boundaries, but at time (processing) greater then 
        2 minutes after window created
            * message inserted in the created window and calculates aggregation 
            (continues previous calculation)
        1. sending message with new boundaries
            * new window created with appropriate boundaries and calculates 
            aggregation (from starting)
        ---------------------------------------------
        **Conclusion:** _window based on event time is created but never closed, so every late arrival will be added to existed window calculation/aggregation_
1. windowedBy operator with session window
    * events emitted immediately after received (no suppress)
    1. With TimeExtractor (window size: 2 minutes between opening new window) **process time**
        1. No **until** parameter (means, session window won't be re-opened - every new event 
                with current starting boundaries added to this window)
            1. sending message with time extracted older then processing time
                * opened window with 2 minute boundary and calculates aggregation 
                (from starting)
            1. sending message with time extracted older then processing time and 
            inside the window boundaries, but at time (processing) less then 2 minutes 
            after window created
                * message inserted in the created window with event time as start and end time +2 minutes
                 and calculates aggregation (continues previous calculation)
            1. sending message with time extracted older then processing time and 
                inside the window boundaries, but at time (processing) greater then 2 minutes 
                after window created
                * message inserted in the created window and calculates aggregation 
                (continues previous calculation)
            1. sending message with new boundaries
                * new window created with appropriate boundaries 
                (start and end time greater then previous window boundaries) and calculates 
                aggregation (from starting)
            * **Notes:**
                1. changes will cause window's upper boundary (end time) to go up (adding to last window inactivityGap value)
                1. For some reason, receives additional event with same window boundaries, but with **null** value, so need to filter it (why?)
                1. When we have same key and 2 windows: key:key1, time: s0 -> **key1, s0-s3** window created, receiving next message with 
                the same key and at time s1, aggregates data with first window, but changes upper boundary **key1, s0-s5**. 
                Sending message with the same key, but at time s6, creates new window: **key1, s6-s8**.
                From this moment any message which arrives at time between s0 and s6 (exclusive) will be added to first window 
                and increases its upper boundary (meaningless), but messages between s6 and s8 will be added into second window 
                and increases its upper boundary 
        ---------------------------------------------
        **Conclusion:** _window based on event time is created but never closed, so every late arrival will be added to existed window calculation/aggregation_