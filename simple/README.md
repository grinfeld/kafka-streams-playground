Kafka-Streams and Windowing
--------------------

1. windowedBy operator with static window
    * events emitted immediately after received (no suppress)
    1. No TimeExtractor (window size: 2 minutes) **process time**
        * window is opened every 2 minutes. Actually, we don't have late arrival messages, so 
        messages entered inside 2 minute window
    1. With TimeExtractor (window size: 2 minutes) - **event time**
        1. sending message with time extracted older then processing time
            * opened window with 2 minute boundary
        1. sending message with time extracted older then processing time and inside the window boundaries, but at time (processing) less then 2 minutes after window created
            * message inserted in the created window
        1. sending message with time extracted older then processing time and inside the window boundaries, but at time (processing) greater then 2 minutes after window created
            * message inserted in the created window
        ---------------------------------------------
        **Conclusion:** _window based on event time is created but never closed, so every late arrival will be added to existed window_
1. join with window