import React from 'react';
import { render } from '@testing-library/react';
import App from './App';

test('renders error', () => {
  const { getByText } = render(<App />);
  const errorElement = getByText(/lError:/i);
  expect(errorElement).toBeInTheDocument();
});
