import React from 'react';
import { store, view } from '@risingstack/react-easy-state';

const databaseState = store({
    dbName: "",
    workflowDot: "graph{}",
});

export default databaseState;