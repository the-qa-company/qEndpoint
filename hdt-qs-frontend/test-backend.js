const axios= require('axios')

for (let index = 0; index < 100; index++) {
        axios.get('http://localhost:1234/api/endpoint/sparql',{ params: { query: 'select * where {?s ?p ?o }' } })
        .then((response) => {
            console.log(response.data);
            var res = []
            response.data.results.bindings.map( (b) => {
                res.push(b)
            })
        }, (error) => {
        console.log(error);
        });
}
