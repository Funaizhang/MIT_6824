iter=0;
while [ $iter -le 50 ];
do
    ((iter++));
    if go test -run 3B >> 3B ; then
        echo "Pass ${iter}-th iteration"
    else
        echo "Fail ${iter}-th iteration"
    fi
done;