

import React from 'react';
import { withRouter, Switch } from 'react-router-dom';
import AppRoute from './utils/AppRoute';
import Home from './views/Home'
import DefaultLayout from './layouts/DefaultLayout';
class App extends React.Component {

  render() {
    return (
          <Switch>
            <AppRoute exact path="/" component={Home} layout={DefaultLayout} />
          </Switch>
    );
  }
}

export default withRouter(props => <App {...props} />);

