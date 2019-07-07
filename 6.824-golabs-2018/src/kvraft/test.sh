iter=1;
while true;
do
    time go test -run TestSnapshotRecoverManyClients3B > TestSnapshotRecoverManyClients3B || break;
    echo "Pass ${iter}-th iteration"
    ((iter++));
done;
echo "The program fails at the ${iter}-th iteration";