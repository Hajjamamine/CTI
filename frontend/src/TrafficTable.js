// src/TrafficTable.js

import React, { useEffect, useState } from "react";
import {
  Card,
  CardContent,
  Grid,
  Typography,
  Avatar,
  Chip,
  Box,
  Zoom,
} from "@mui/material";
import ShieldIcon from '@mui/icons-material/Shield';
import WarningIcon from '@mui/icons-material/Warning';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import './TrafficTable.css';

const TrafficTable = () => {
  const [data, setData] = useState([]);

  useEffect(() => {
    const fetchData = () => {
      fetch("http://127.0.0.1:8000/api/predictions/")
        .then((response) => response.json())
        .then((data) => setData(data))
        .catch((error) => console.error("Error fetching data:", error));
    };

    fetchData(); // Initial call
    const interval = setInterval(fetchData, 10000); // Every 10 seconds

    return () => clearInterval(interval); // Cleanup on unmount
  }, []);


  return (
    <Card
      sx={{
        background: 'linear-gradient(120deg, #181a2a 60%, #23244a 100%)',
        boxShadow: '0 0 60px #00ffff55',
        borderRadius: 8,
        mt: 4,
        p: 2,
        border: '2px solid #00ffff44',
        position: 'relative',
        overflow: 'visible',
      }}
    >
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
          <Avatar sx={{ bgcolor: '#ff00cc', width: 48, height: 48, boxShadow: '0 0 20px #ff00cc' }}>
            <SmartToyIcon sx={{ fontSize: 32, color: '#fff' }} />
          </Avatar>
          <Typography variant="h5" sx={{ ml: 2, fontWeight: 700, letterSpacing: 2, color: '#00ffff', textShadow: '0 0 10px #00ffff' }}>
            AI Threat Grid
          </Typography>
        </Box>
        <Grid container spacing={2}>
          {data.map((item, index) => (
            <Zoom in={true} key={index}>
              <Grid item xs={12} sm={6} md={4} lg={3}>
                <Card
                  sx={{
                    background: item.prediction === 0
                      ? 'linear-gradient(120deg, #00ff88 10%, #181a2a 90%)'
                      : 'linear-gradient(120deg, #ff4444 10%, #23244a 90%)',
                    color: '#fff',
                    borderRadius: 4,
                    boxShadow: item.prediction === 0
                      ? '0 0 20px #00ff88'
                      : '0 0 20px #ff4444',
                    p: 2,
                    mb: 2,
                    position: 'relative',
                    border: '1.5px solid #00ffff44',
                    transition: 'background 0.3s',
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                    {item.prediction === 0 ? (
                      <Chip
                        icon={<ShieldIcon style={{ color: '#00ff88' }} />}
                        label="Safe"
                        sx={{
                          background: 'rgba(0,255,88,0.2)',
                          color: '#00ff88',
                          fontWeight: 'bold',
                          boxShadow: '0 0 5px #00ff88',
                        }}
                      />
                    ) : (
                      <Chip
                        icon={<WarningIcon style={{ color: '#ff4444' }} />}
                        label="Malicious"
                        sx={{
                          background: 'rgba(255,44,44,0.2)',
                          color: '#ff4444',
                          fontWeight: 'bold',
                          boxShadow: '0 0 5px #ff4444',
                        }}
                      />
                    )}
                  </Box>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1, mb: 1 }}>
                    <span style={{ color: '#00ffff' }}>Port:</span> {item.Destination_Port}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1 }}>
                    <span style={{ color: '#ff00cc' }}>Flow:</span> {item.Flow_Duration}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1 }}>
                    <span style={{ color: '#00ffff' }}>Fwd Packets:</span> {item.Total_Fwd_Packets}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1 }}>
                    <span style={{ color: '#ff00cc' }}>Bwd Packets:</span> {item.Total_Backward_Packets}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1 }}>
                    <span style={{ color: '#00ffff' }}>Fwd Header:</span> {item.Fwd_Header_Length}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1 }}>
                    <span style={{ color: '#ff00cc' }}>Bwd Header:</span> {item.Bwd_Header_Length}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1 }}>
                    <span style={{ color: '#00ffff' }}>Fwd/s:</span> {item.Fwd_Packets_s}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600, letterSpacing: 1 }}>
                    <span style={{ color: '#ff00cc' }}>Bwd/s:</span> {item.Bwd_Packets_s}
                  </Typography>
                </Card>
              </Grid>
            </Zoom>
          ))}
        </Grid>
      </CardContent>
    </Card>
  );
};

export default TrafficTable;
