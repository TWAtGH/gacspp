SELECT queuedat, count(id)
FROM transfers
GROUP BY queuedat
ORDER BY queuedat;