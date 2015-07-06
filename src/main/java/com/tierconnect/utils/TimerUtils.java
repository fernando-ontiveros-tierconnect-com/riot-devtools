package com.tierconnect.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by fernando on 6/21/15.
 */
public class TimerUtils {
    List<Long> times = new ArrayList<Long>();

    public void mark()
    {
        times.add( System.currentTimeMillis() );
    }

    public long getLastDelt()
    {
        return times.get( times.size() - 1 ) - times.get( times.size() - 2 );
    }

    public long getTotalDelt()
    {
        return times.get( times.size() - 1 ) - times.get( 0 );
    }
}
