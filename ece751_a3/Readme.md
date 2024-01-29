StorageNode -> To start the primary and backup

-> create child node under parent znode
-> get the list of children, first index is the primary
-> if primary make an RPC to set the primary as true
-> if backup, make an rpc to copy the map from primary - (backup)


Handler

get
regular

put
lock
put in primary
put in backup -> rpc
unlock

put in backup
lock
put
unlock


copy from primary
called by the backend
lock
so rpc the primary
unlock

getMap
used by copy from primary

watcher
same as client

