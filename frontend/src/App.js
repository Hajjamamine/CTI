


import React, { useState } from "react";
import TrafficTable from "./TrafficTable";
import {
  CssBaseline,
  Container,
  Box,
  Typography,
  Fade,
  Avatar,
  IconButton,
  Switch,
  Tooltip,
  useMediaQuery,
} from "@mui/material";
import { createTheme, ThemeProvider } from "@mui/material/styles";
import BlurOnIcon from '@mui/icons-material/BlurOn';
import LightModeIcon from '@mui/icons-material/LightMode';
import DarkModeIcon from '@mui/icons-material/DarkMode';


const getTheme = (mode) => createTheme({
  palette: {
    mode,
    primary: {
      main: mode === "dark" ? "#00ffff" : "#23244a",
    },
    background: {
      default: mode === "dark" ? "#181a2a" : "#f5f7fa",
      paper: mode === "dark" ? "#23244a" : "#ffffffcc",
    },
    secondary: {
      main: mode === "dark" ? "#ff00cc" : "#ff8800",
    },
  },
  typography: {
    fontFamily: 'Orbitron, Roboto, Arial, sans-serif',
    h2: {
      fontWeight: 700,
      letterSpacing: 2,
      textShadow: mode === "dark"
        ? "0 0 20px #00ffff, 0 0 40px #ff00cc"
        : "0 0 10px #ff8800, 0 0 20px #23244a",
      fontSize: '2.8rem',
      color: mode === "dark" ? '#00ffff' : '#23244a',
    },
  },
});




function App() {
  const prefersDarkMode = useMediaQuery('(prefers-color-scheme: dark)');
  const [mode, setMode] = useState(prefersDarkMode ? 'dark' : 'light');

  // Animated background colors for both modes
  const backgroundGradient = mode === 'dark'
    ? "linear-gradient(135deg, #0f0c29, #302b63, #24243e)"
    : "linear-gradient(135deg, #f5f7fa, #e2eafc, #fff1eb)";

  // Particle colors for both modes
  const particleGradient = mode === 'dark'
    ? 'linear-gradient(135deg, #00ffff 40%, #ff00cc 100%)'
    : 'linear-gradient(135deg, #ff8800 40%, #23244a 100%)';

  const theme = getTheme(mode);

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      {/* Animated Futuristic Background */}
      <Box
        sx={{
          minHeight: "100vh",
          width: "100vw",
          position: "fixed",
          top: 0,
          left: 0,
          zIndex: -1,
          overflow: "hidden",
          background: backgroundGradient,
          transition: 'background 0.8s',
        }}
      >
        <svg style={{position: 'absolute', width: '100%', height: '100%'}}>
          <defs>
            <radialGradient id="neonGradient" cx="50%" cy="50%" r="80%">
              <stop offset="0%" stopColor={mode === 'dark' ? "#00ffff" : "#ff8800"} stopOpacity="0.5" />
              <stop offset="60%" stopColor={mode === 'dark' ? "#ff00cc" : "#23244a"} stopOpacity="0.2" />
              <stop offset="100%" stopColor={mode === 'dark' ? "#181a2a" : "#f5f7fa"} stopOpacity="0.8" />
            </radialGradient>
          </defs>
          <ellipse cx="50%" cy="40%" rx="60%" ry="30%" fill="url(#neonGradient)" />
        </svg>
        {/* Animated floating particles */}
        {[...Array(20)].map((_, i) => (
          <Box
            key={i}
            sx={{
              position: 'absolute',
              top: `${10 + Math.random() * 80}%`,
              left: `${5 + Math.random() * 90}%`,
              width: 12 + Math.random() * 18,
              height: 12 + Math.random() * 18,
              borderRadius: '50%',
              background: particleGradient,
              opacity: 0.18 + Math.random() * 0.22,
              filter: 'blur(2px)',
              animation: `float${i} 8s ease-in-out infinite`,
              '@keyframes float${i}': {
                '0%': { transform: 'translateY(0px)' },
                '50%': { transform: `translateY(${Math.random() * 40 - 20}px)` },
                '100%': { transform: 'translateY(0px)' },
              },
            }}
          />
        ))}
      </Box>

      {/* Glassmorphism Card Container */}
      <Box
        sx={{
          minHeight: "100vh",
          background: mode === 'dark' ? "rgba(20, 20, 40, 0.7)" : "rgba(255,255,255,0.7)",
          backdropFilter: "blur(12px)",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          zIndex: 1,
          transition: 'background 0.8s',
        }}
      >
        <Container maxWidth="lg" sx={{
          boxShadow: mode === 'dark' ? '0 0 60px #00ffff44' : '0 0 60px #ff880044',
          borderRadius: 6,
          p: 4,
          mt: 6,
          transition: 'box-shadow 0.8s',
        }}>
          {/* Dark/Light Mode Toggle */}
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', mb: 1 }}>
            <Tooltip title={mode === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode'}>
              <IconButton onClick={() => setMode(mode === 'dark' ? 'light' : 'dark')} sx={{ mr: 1 }}>
                {mode === 'dark' ? <LightModeIcon sx={{ color: '#ff8800' }} /> : <DarkModeIcon sx={{ color: '#23244a' }} />}
              </IconButton>
            </Tooltip>
            <Switch
              checked={mode === 'dark'}
              onChange={() => setMode(mode === 'dark' ? 'light' : 'dark')}
              color="primary"
              inputProps={{ 'aria-label': 'toggle dark/light mode' }}
            />
          </Box>
          <Fade in timeout={1200}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', mb: 2 }}>
              <Avatar sx={{ bgcolor: 'primary.main', width: 56, height: 56, boxShadow: mode === 'dark' ? '0 0 20px #00ffff' : '0 0 20px #ff8800' }}>
                <BlurOnIcon sx={{ fontSize: 38, color: mode === 'dark' ? '#181a2a' : '#fff1eb' }} />
              </Avatar>
              <Typography
                variant="h2"
                align="center"
                gutterBottom
                sx={{
                  ml: 2,
                  fontFamily: 'Orbitron, Roboto, Arial, sans-serif',
                  fontWeight: 900,
                  letterSpacing: 3,
                  textTransform: 'uppercase',
                  background: 'linear-gradient(90deg, #00ffff 0%, #ff00cc 100%)',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent',
                  textShadow: mode === 'dark'
                    ? '0 0 30px #00ffff, 0 0 60px #ff00cc'
                    : '0 0 20px #ff8800, 0 0 40px #23244a',
                  fontSize: { xs: '2.2rem', sm: '3.2rem', md: '3.8rem' },
                  transition: 'all 0.8s',
                }}
              >
                Cyber Threat Intelligence
              </Typography>
            </Box>
          </Fade>
          <Fade in timeout={1800}>
            <Box>
              <TrafficTable />
            </Box>
          </Fade>
        </Container>
      </Box>
    </ThemeProvider>
  );
}

export default App;
