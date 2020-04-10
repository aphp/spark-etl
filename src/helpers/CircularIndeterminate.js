import React from 'react';
import CircularProgress from '@material-ui/core/CircularProgress';
import Box from '@material-ui/core/Box';
import Typography from '@material-ui/core/Typography';

export default function CircularIndeterminate(props) {
  const { size } = props;

  return (
    <Box display="flex" flexDirection="column" style={{ width: '100%' }}>
      <Box m="auto" p="50px"><CircularProgress size={size}/></Box>
      <Box m="auto"><Typography component="div" bgcolor="background.paper">Loading...</Typography></Box>
    </Box>
  );
}