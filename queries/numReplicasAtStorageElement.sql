SELECT r.createdat, r.expiredat, count(r.id)
FROM Replicas r, StorageElements s
WHERE r.storageelementid = s.id
AND s.name = '<storageelementname>'
GROUP BY createdat, expiredat
ORDER BY createdat;