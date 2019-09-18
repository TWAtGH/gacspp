SELECT startedat, count(id)
FROM transfers
GROUP BY startedat
ORDER BY startedat;