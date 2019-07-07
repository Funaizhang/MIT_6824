iter=1;
while true;
do
    time go test -run TestSnapshotRPC3B > TestSnapshotRPC3B || break;
    echo "Pass ${iter}-th iteration"
    ((iter++));
done;
echo "The program fails at the ${iter}-th iteration";