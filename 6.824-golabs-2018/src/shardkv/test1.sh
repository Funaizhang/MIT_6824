iter=0;
while [ $iter -le 70 ];
do
    ((iter++));
    if go test -run TestStaticShards >> TestStaticShards; then
        echo "Pass ${iter}-th iteration"
    else
        echo "Fail ${iter}-th iteration"
    fi
done;