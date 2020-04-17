import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Modal from '@material-ui/core/Modal';
import TextField from '@material-ui/core/TextField';
import Button from '@material-ui/core/Button';
import Box from '@material-ui/core/Box';
import Typography from '@material-ui/core/Typography';
import EditIcon from '@material-ui/icons/Edit';

function getModalStyle() {
  const top = 50;
  const left = 50;

  return {
    top: `${top}%`,
    left: `${left}%`,
    transform: `translate(-${top}%, -${left}%)`,
  };
}

const useStyles = makeStyles((theme) => ({
  paper: {
    position: 'absolute',
    width: 400,
    backgroundColor: theme.palette.background.paper,
    border: '2px solid #000',
    boxShadow: theme.shadows[5],
    padding: theme.spacing(2, 4, 3),
  },
  form: {
    '& > *': {
      margin: theme.spacing(1),
      width: '25ch',
    },
  },
}));

export default function SimpleModal({ authToken, setAuthToken }) {
  const classes = useStyles();
  // getModalStyle is not a pure function, we roll the style only on the first render
  const [modalStyle] = React.useState(getModalStyle);
  const [open, setOpen] = React.useState(false);
  const [value, setValue] = React.useState(authToken);

  const handleOpen = () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  const onChange = (e) => {
    setValue(e.target.value);
  };

  const onKeyPress = (e) => {
    if (e.charCode === 13) { // enter key pressed
      e.preventDefault();
      submit();
    }
  }

  const submit = () => {
    setAuthToken(value);
    handleClose();
  }

  const body = (
    <Box style={modalStyle} className={classes.paper}>
      <Typography component="p" id="simple-modal-title">Input auth token to edit</Typography>
      <form className={classes.form} noValidate autoComplete="off">
        <TextField id="standard-basic" label="Token" required value={value} onChange={onChange} onKeyPress={onKeyPress} autoFocus={true}/>
        <Button variant="contained" color="primary" onClick={submit}>
          Save
        </Button>
      </form>
    </Box>
  );

  return (
    <div>
      <EditIcon onClick={handleOpen} style={{cursor:'pointer'}}/>
      <Modal
        open={open}
        onClose={handleClose}
        aria-labelledby="simple-modal-title"
        aria-describedby="simple-modal-description">
        {body}
      </Modal>
    </div>
  );
}