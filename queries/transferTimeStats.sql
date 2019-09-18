SELECT queuedat, avg(startedat - queuedat), avg(finishedat - queuedat) 
FROM transfers
GROUP BY queuedat
ORDER BY queuedat;