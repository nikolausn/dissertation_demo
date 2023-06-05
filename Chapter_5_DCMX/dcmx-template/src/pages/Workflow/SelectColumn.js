import Page from 'material-ui-shell/lib/containers/Page'
import React, { Component } from 'react'
import { useIntl } from 'react-intl'
import DropdownList from "react-widgets/DropdownList";
import { store, view } from '@risingstack/react-easy-state';
import dynamic from 'next/dynamic';
import { Container } from '@mui/material';

import databaseState from '../SelectDatabase/DatabaseState';
import workflowStore from './WorkflowState'


import "react-widgets/styles.css";


export default view(() => (
    <DropdownList
        data={workflowStore.columnList}
        value={workflowStore.columnRecipe}
        dataKey='id'
        textField='name'
        onChange={(value) => workflowStore.changeColumn(value.id, value)}
    />
));
