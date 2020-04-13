// See source origin: https://blog.logrocket.com/the-complete-guide-to-building-inline-editable-ui-in-react/
import React, { useState, useEffect } from "react";

const Editable = ({
  text,
  type,
  placeholder,
  children,
  childRef,
  ...props
}) => {
  const [isEditing, setEditing] = useState(false);

  /*
    using use effect, when isEditing state is changing, check whether it is set to true, if true, then focus on the reference element
  */
  useEffect(() => {
    if (childRef && childRef.current && isEditing === true) {
      childRef.current.focus();
    }
  }, [isEditing, childRef]);

  const handleKeyDown = (event, type) => {
    const { key } = event;
    const keys = ["Escape", "Tab"];
    const enterKey = "Enter";
    const allKeys = [...keys, enterKey]; // All keys array

  /*
    - For textarea, check only Escape and Tab key and set the state to false
    - For everything else, all three keys will set the state to false
  */
    if ((type === "textarea" && keys.indexOf(key) > -1) ||(type !== "textarea" && allKeys.indexOf(key) > -1)) {
      setEditing(false);
    }
  };
  return (
    <section {...props}>
      {isEditing ? (
        <div
          onBlur={() => setEditing(false)}
          onKeyDown={e => handleKeyDown(e, type)}>
          {children}
        </div>
      ) : (
        <div onClick={() => setEditing(true)} style={{cursor:'pointer'}}>
          <span dangerouslySetInnerHTML={{ __html: text}}>
          </span>
        </div>
      )}
    </section>
  );
};


export default Editable;