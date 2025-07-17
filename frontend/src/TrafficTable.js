// src/TrafficTable.js
import React, { useEffect, useState } from "react";
import "./TrafficTable.css";

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
    <div className="App">
      <h1>Network Traffic Predictions</h1>
      <table>
        <thead>
          <tr>
            <th>Destination Port</th>
            <th>Flow Duration</th>
            <th>Total Fwd Packets</th>
            <th>Total Bwd Packets</th>
            <th>Fwd Header Length</th>
            <th>Bwd Header Length</th>
            <th>Fwd Packets/s</th>
            <th>Bwd Packets/s</th>
            <th>Prediction</th>
          </tr>
        </thead>
        <tbody>
          {data.map((item, index) => (
            <tr
              key={index}
              className={
                item.prediction === 0 ? "safe" : "malicious"
              }
            >
              <td>{item.Destination_Port}</td>
              <td>{item.Flow_Duration}</td>
              <td>{item.Total_Fwd_Packets}</td>
              <td>{item.Total_Backward_Packets}</td>
              <td>{item.Fwd_Header_Length}</td>
              <td>{item.Bwd_Header_Length}</td>
              <td>{item.Fwd_Packets_s}</td>
              <td>{item.Bwd_Packets_s}</td>
              <td>{item.prediction}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default TrafficTable;
