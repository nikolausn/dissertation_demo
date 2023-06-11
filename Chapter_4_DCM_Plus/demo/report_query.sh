echo "Rendering visualization for a serial workflow"
or2yw -t serial -i OR-history.json -ot pdf -o OR-history-serial.pdf
echo "output rendered at OR-history-serial.pdf"
echo "Rendering visualization for a parallel workflow"
or2yw -t parallel -i OR-history.json -ot pdf -o OR-history-parallel.pdf
echo "output rendered at OR-history-parallel.pdf"
echo "Rendering visualization for a merge workflow"
or2yw -t merge -i OR-history.json -ot pdf -o OR-history-merge.pdf
echo "output rendered at OR-history-merge.pdf, sub_ops_<number>.json will also be created as result of collapsing workflow"
