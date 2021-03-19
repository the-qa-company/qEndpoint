import JSONTree from 'react-json-tree'

import '../assets/css/home.css'
const CardRes = ({result}) => {
    const resultList = result.length ? (
        result.map( res => {
            return(
                <div>
                    <JSONTree data={res} />
                </div>
            )
        })
    ):(
        <p>Empty!!!</p>
    )
    return(
        <div className="container">
            <div className="container-inside">
                {resultList}
            </div>
        </div>
    )
}

export default CardRes