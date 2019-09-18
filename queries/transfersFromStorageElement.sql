SELECT count(t.id)
FROM transfers t, replicas r, storageelements s
WHERE t.srcReplicaId = r.id AND r.storageElementId = s.id AND s.name = '<storageelementname>';
