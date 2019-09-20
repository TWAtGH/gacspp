SELECT startedat, avg(startedat - queuedat), avg(finishedat - queuedat) 
FROM transfers
GROUP BY startedat
ORDER BY startedat;