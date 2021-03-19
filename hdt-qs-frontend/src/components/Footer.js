import React from 'react';

import '../assets/css/home.css'
class Footer extends React.Component{

    render(){
        return (
            <div id="footer" className="footer">
                <div style={{"padding":30, "color":"white","text-align":"center"}}>&copy; {new Date().getFullYear()} The QA Company, all rights reserved</div>
            </div>
        )
    }
}

export default Footer