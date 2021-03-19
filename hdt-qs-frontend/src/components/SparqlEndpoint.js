import React, {Component} from 'react'
import Yasgui from "@triply/yasgui";
import "@triply/yasgui/build/yasgui.min.css";
import './SparqlEndpoint.css';

class SparqlEndpoint extends Component {

    constructor(props) {
        super(props);
        this.state = {
            value: 5, //indicates if the answer or the query is displayed
        };
    }

    componentDidMount() {
        const yasgui = new Yasgui(document.getElementById("yasgui"),
            {
                requestConfig: {
                        endpoint: 'http://localhost:1234/api/endpoint/sparql',
                        // Example of using a getter function to define the headers field:
                        headers: () => ({
                            'timeout': this.state.value
                        }),
                        method: 'GET',

                },
                copyEndpointOnNewTab: false,
        });
        var tab = yasgui.getTab();
        while (tab !== undefined){
            tab.close();
            tab = yasgui.getTab();
        }
        yasgui.addTab(
            true, // set as active tab
            { ...yasgui.config}
        );
    }

    handleChange(event) {
        this.setState({value: event.target.value});
    }

    render() {

        return (
            <div style={{"paddingRight": 300, "paddingLeft":300,"paddingBottom":250}}>
                <h1>HDT Query Service</h1>
                <div>This interface is a SPARQL endpoint over the HDT wikidata index. Just type your SPARQL query
                    and execute it.
                </div>
                <br></br>
                <div style={{'fontWeight': 'bold'}}>Timeout (in seconds):
                <input style={{'fontWeight': 'bold', 'fontSize': '15px', 'marginLeft': '10px'}} type="text" value={this.state.value} onChange={this.handleChange.bind(this)} />
                </div>

                <br></br>
                <div>
                    <div id="yasgui" />
                </div>
            </div>
        );
    }
}


export default SparqlEndpoint
