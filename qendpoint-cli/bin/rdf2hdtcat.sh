#!/bin/bash

function showhelp {
	echo
	echo "Script to serialize a big RDF file in n-triples format into HDT"
	echo "It splits the file in N parts, compress each one with rdf2hdt, and merges them iteratively with hdtCat."
	echo
	echo "Usage $0 [OPTION]"
	echo
    echo "  -c, --catscript location of hdtCat script  (assuming it's in PATH by by default)"
    echo "  -i, --input     input file (input.rdf by default)"
	echo "  -h, --help      shows this help and exits"
	echo "  -n, --number    number of files to split FILE (2 by default)"
    echo "  -o, --output    output file (output.hdt by default)"
    echo "  -p, --parallel  number of threads to serialize RDF into HDT in parallel (1 by default)"
    echo "  -r, --rdf2hdt   location of rdf2hdt script (assuming it's in PATH by default)"
	echo
}

# Defaults
declare rdf2hdt="bin/rdf2hdt.sh"
declare hdtCat="bin/hdtCat.sh"
declare input="input.rdf"
declare -i lines
declare output="output.hdt"
declare -i splits=2
declare -i threads=1

getopt --test > /dev/null
if [[ $? -eq 4 ]]; then
    # enhanced getopt works
    OPTIONS=c:i:hn:o:p:r:
    LONGOPTIONS=cat:,input:,help,number:,output:,parallel:,rdf2hdt:
    COMMAND=$(getopt -o $OPTIONS -l $LONGOPTIONS -n "$0" -- "$@")
    if [[ $? -ne 0 ]]; then
    	exit 2
    fi
    eval set -- "$COMMAND"
else
	echo "Enhanced getopt not supported. Brace yourself, this is not tested, but it should work :-)"
fi

while true; do
	case "$1" in
        -c|--cat)
            hdtCat=$2
            shift 2
            ;;
        -i|--input)
            input=$2
            shift 2
            ;;
		-n|--number)
			splits=$2
			shift 2
			;;
        -o|--output)
            output=$2
            shift 2
            ;;
        -p|--parallel)
            threads=$2
            shift 2
            ;;
        -r|--rdf2hdt)
            rdf2hdt=$2
            shift 2
            ;;
		--)
			shift
			break
			;;
		*)
			showhelp
			exit 0
			;;
	esac
done

total_lines=$(wc -l < $input)
lines=($total_lines+$splits-1)/$splits #Set number of lines rounding up

split -l $lines $input "$input"_split_

echo "***************************************************************"
echo "Serializing into HDT $splits files using $threads threads"
echo "***************************************************************"
echo -n "$input"_split_* | xargs -I{} -d' ' -P$threads $rdf2hdt -rdftype ntriples {} {}_"$splits".hdt

for (( i=$splits; i>1; i=i/2 )); do
    echo "***************************************************************"
    echo "Merging $i hdt files: " "$input"_split_*_"$i".hdt
    echo "***************************************************************"
    echo -n "$input"_split_*_"$i".hdt | xargs -d' ' -n2 -P$threads bash -c 'temp=${2%_*.hdt} ; bin/hdtCat.sh $1 $2 ${1%_*.hdt}_${temp#*split_}_$0.hdt' $((i/2))
    command='temp=${2%_*.hdt} ; '$hdtCat' $1 $2 ${1%_*.hdt}_${temp#*split_}_$0.hdt'
    echo -n "$input"_split_*_"$i".hdt | xargs -d' ' -n2 -P$threads bash -c "$command" $((i/2))
done

echo "***************************************************************"
echo "Moving output to '$output' file"
echo "***************************************************************"
mv "$input"_split*_1.hdt "$output"

echo "***************************************************************"
echo "Cleaning up split files"
echo "***************************************************************"
rm "$input"_split_*
