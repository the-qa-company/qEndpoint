import JSONTree from 'react-json-tree'

import '../assets/css/home.css'
const Results = ({results}) => {
    var count = 0
    const resultList = results.length ? (
        results.map( res => {
            count++
            return(
                <div key={count}>
                    <h2>Query {count}:</h2>
                    <JSONTree data={res} />
                </div>
            )
        })
    ):(
        <p>Write a query to show the results...</p>
    )
    return(
        <div className="container">
            <div className="container-inside">
                {resultList}
            </div>
        </div>
    )
}

export default Results