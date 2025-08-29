SELECT 
    h.hostid,
    h.name AS host_name,
    p.eventid,
    FROM_UNIXTIME(p.clock) AS problem_start,
    FROM_UNIXTIME(p.r_clock) AS problem_end,
    TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(p.clock), FROM_UNIXTIME(p.r_clock)) AS problem_duration_seconds,
    CASE 
        WHEN p.r_clock = 0 THEN 'Ongoing'
        ELSE 'Resolved'
    END AS problem_status,
    t.description AS trigger_description,
    t.priority AS severity,
    s.total_downtime_events,
    s.uptime_percentage,
    inf.interfaceid,
    inf.province,
    inf.port,
    inf.available,
    inf.error,
    inf.ip,
    inf.problemstatus,
    inf.availabilitystatus,
    inf.avg_signal_strength,
    inf.signal_strength
FROM problem p
JOIN events e ON p.eventid = e.eventid
JOIN triggers t ON e.objectid = t.triggerid
JOIN functions f ON t.triggerid = f.triggerid
JOIN items i ON f.itemid = i.itemid
JOIN hosts h ON i.hostid = h.hostid

LEFT JOIN (
    SELECT 
        h1.hostid,
        COUNT(p1.eventid) AS total_downtime_events,
        CAST(
            ROUND(
                (
                    ((120*24*3600) - COALESCE(SUM(TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(p1.clock), FROM_UNIXTIME(p1.r_clock))),0))
                    / (120*24*3600)
                ) * 100, 2
            ) AS DECIMAL(10,2)
        ) AS uptime_percentage
    FROM problem p1
    JOIN events e1 ON p1.eventid = e1.eventid
    JOIN triggers t1 ON e1.objectid = t1.triggerid
    JOIN functions f1 ON t1.triggerid = f1.triggerid
    JOIN items i1 ON f1.itemid = i1.itemid
    JOIN hosts h1 ON i1.hostid = h1.hostid
    WHERE p1.clock >= UNIX_TIMESTAMP(NOW() - INTERVAL 120 DAY)
      AND p1.r_clock > 0
    GROUP BY h1.hostid
) s ON h.hostid = s.hostid

LEFT JOIN (
    SELECT
        h2.hostid,
        i2.interfaceid,
        hg.name AS province,
        i2.port,
        i2.available,
        i2.error,
        i2.ip,
        CASE 
            WHEN t2.value = 1 THEN 'Active'
            WHEN t2.value = 0 THEN 'Resolved'
        END AS problemstatus,
        CASE 
            WHEN inter2.available = 0 THEN 'Unknown'
            WHEN inter2.available = 1 THEN 'Stable'
            WHEN inter2.available = 2 THEN 'Not Stable'
        END AS availabilitystatus,
        tr.value_avg AS avg_signal_strength,
        CASE 
            WHEN tr.value_avg > -70 THEN 'Excellent'
            WHEN tr.value_avg BETWEEN -70 AND -85 THEN 'Good'
            WHEN tr.value_avg BETWEEN -86 AND -100 THEN 'Fair'
            WHEN tr.value_avg < -100 THEN 'Poor'
            WHEN tr.value_avg = -110 THEN 'No Signal'
        END AS signal_strength
    FROM hosts AS h2
    JOIN hosts_groups hg_map ON h2.hostid = hg_map.hostid
    JOIN hstgrp hg ON hg_map.groupid = hg.groupid
    JOIN interface i2 ON h2.hostid = i2.hostid
    JOIN items itm ON h2.hostid = itm.hostid
    LEFT JOIN trends tr ON itm.itemid = tr.itemid
    JOIN functions f2 ON itm.itemid = f2.itemid
    JOIN triggers t2 ON f2.triggerid = t2.triggerid
    JOIN interface inter2 ON h2.hostid = inter2.hostid
) inf ON h.hostid = inf.hostid

WHERE p.clock >= UNIX_TIMESTAMP(NOW() - INTERVAL 120 DAY)
ORDER BY p.clock DESC;
