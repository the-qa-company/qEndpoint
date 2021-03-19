import React from 'react';
import Header from '../components/Header';
import Footer from '../components/Footer';

const DefaultLayout = ({ children }) => (
  <React.Fragment>
    <Header/>
    <main >
      {children}
    </main>
    <Footer />
  </React.Fragment>
);

export default DefaultLayout;  