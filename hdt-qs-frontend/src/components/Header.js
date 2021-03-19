import React from 'react';
import logo from '../assets/images/logo.svg'

import "../assets/css/header.css";
class Header extends React.Component{

    divStyle = {
        color: 'blue',
      };
    render(){
        return (
            <div >
                <ul className="ul-header">
                    <li className="li-header">
                        <img src={logo} height="90" width="500" alt=""></img>
                    </li>
                    <li className="li-header"><a href="https://the-qa-company.com/">About</a></li>
                    <li className="li-header active"><a href="#main-div">Home</a></li>
                </ul>
            </div>
        )
    }
}

export default Header