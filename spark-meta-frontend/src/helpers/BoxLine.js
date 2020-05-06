import React from 'react';
import Box from '@material-ui/core/Box';
import Typography from '@material-ui/core/Typography';

function BoxLine(props) {
  const { items } = props;
  return (
    <Box display="flex" style={{ width: '100%' }}>
      {items.map(i => {
        return <Box m="auto" key={'box-' + i} p={2}>
          <Typography variant={i.variant || "body1"} bgcolor="background.paper">
            {i.text || i}
          </Typography>
        </Box>})}
    </Box>);
}

export default BoxLine;