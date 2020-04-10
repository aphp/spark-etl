import React from "react";
import TextField from '@material-ui/core/TextField';
import Autocomplete from '@material-ui/lab/Autocomplete';

class Select extends React.PureComponent {
  onChangeValue = (event, newValue) => {
    this.props.onChange(newValue);
  }

  render() {
    const { options, label, selectedValue } = this.props;

    if (options.length === 0) {
      return null;
    }

    return (<Autocomplete
      value={selectedValue}
      options={options}
      getOptionLabel={option => option.name}
      onChange={this.onChangeValue}
      autoSelect autoHighlight autoComplete clearOnEscape openOnFocus
      renderInput={(params) => <TextField {...params} label={label} margin="normal" />}
    />);
  }
}

export default Select;