import React from "react";
import TextField from '@material-ui/core/TextField';
import Autocomplete from '@material-ui/lab/Autocomplete';

function Select(props) {
  const { options, label, onChange, selectedValue } = props;

  if (options.length === 0) {
    return null;
  }


  return (<Autocomplete
    value={selectedValue}
    options={options}
    getOptionLabel={option => option.name}
    onChange={(event, newValue) => {
      onChange(newValue);
    }}
    autoSelect autoHighlight autoComplete clearOnEscape openOnFocus
    renderInput={(params) => <TextField {...params} label={label} margin="normal" />}
  />);
}

export default Select;