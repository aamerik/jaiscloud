import { createTheme } from '@mui/material/styles'

/**
 * Material theme for the GCP console, approximating the real Google Cloud
 * Console: Google Blue primary, neutral grays, Roboto and a denser layout than
 * Material's defaults. Only the GCP shell uses this; the AWS/Azure consoles
 * stay on Cloudscape.
 */
export const gcpTheme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: '#1a73e8' },
    secondary: { main: '#5f6368' },
    background: { default: '#f8f9fa', paper: '#ffffff' },
    text: { primary: '#202124', secondary: '#5f6368' },
    divider: '#dadce0',
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
    button: { textTransform: 'none', fontWeight: 500 },
  },
  shape: { borderRadius: 8 },
  components: {
    MuiAppBar: {
      defaultProps: { elevation: 0, color: 'inherit' },
      styleOverrides: {
        root: { backgroundColor: '#ffffff', borderBottom: '1px solid #dadce0' },
      },
    },
    MuiToolbar: { styleOverrides: { root: { minHeight: 48 } } },
    MuiButton: { styleOverrides: { root: { textTransform: 'none' } } },
    MuiListItemButton: {
      styleOverrides: { root: { borderRadius: 8, margin: '0 8px', minHeight: 36 } },
    },
    MuiTableCell: { styleOverrides: { root: { padding: '8px 16px' } } },
  },
})
