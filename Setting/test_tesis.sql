-- phpMyAdmin SQL Dump
-- version 4.8.5
-- https://www.phpmyadmin.net/
--
-- Servidor: 127.0.0.1
-- Tiempo de generación: 08-07-2021 a las 05:46:29
-- Versión del servidor: 10.1.40-MariaDB
-- Versión de PHP: 7.3.5

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Base de datos: `blockchain_db`
--
-- --------------------------------------------------------
--
--

CREATE TABLE `warning_contracts_created` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `warning_degree_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `warning_degree_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `warning_balance_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `warning_balance_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `warning_gasPrice_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `warning_gasPrice_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `gasPrice_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_gasPrice_out` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `gasPrice_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_gasPrice_in` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `balance_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_balance_out` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `balance_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_balance_in` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `degree_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_degree_out` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `degree_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_degree_in` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_invocaciones` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_threshold_invocaciones` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_transferencias` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_threshold_transferencias` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `transferencias` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_value_transferido` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `invocaciones` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_invocaciones` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `contracts_created` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_blockNumber` text NOT NULL,
  `4_address` text NOT NULL,
  `5_contracts_created` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_degree_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_address` text NOT NULL,
  `4_thr_degree_out` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_degree_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_address` text NOT NULL,
  `4_thr_degree_in` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_gasPrice_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_address` text NOT NULL,
  `4_thr_gasPrice_in` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_gasPrice_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_address` text NOT NULL,
  `4_thr_gasPrice_out` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_balance_out` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_address` text NOT NULL,
  `4_thr_balance_out` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `threshold_balance_in` (
  `1_primary_key` text NOT NULL,
  `2_sd` text NOT NULL,
  `3_address` text NOT NULL,
  `4_thr_balance_in` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------
--
--

CREATE TABLE `is_owner` (
  `1_primary_key` text NOT NULL,
  `2_address` text NOT NULL,
  `3_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
--
--

CREATE TABLE `invoke` (
  `1_primary_key` text NOT NULL,
  `2_address` text NOT NULL,
  `3_address` text NOT NULL,
  `4_block_number` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
--
--

CREATE TABLE `hyp_same_person` (
  `1_primary_key` text NOT NULL,
  `2_address` text NOT NULL,
  `3_address` text NOT NULL,
  `4_block_number` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
--
--

CREATE TABLE `hyp_malicious` (
  `1_primary_key` text NOT NULL,
  `2_address` text NOT NULL,
  `3_block_number` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
--
--

CREATE TABLE `mapping` (
  `1_primary_key` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------
--
--
--

CREATE TABLE `null_info` (
  `1_primary_key` text NOT NULL,
  `2_value` text NOT NULL,
  `3_table_name` text NOT NULL,
  `4_foreign_key` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
--
--

CREATE TABLE `node` (
  `1_primary_key` text NOT NULL,
  `2_id` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `edge`
--

CREATE TABLE `edge` (
  `1_primary_key` text NOT NULL,
  `2_from` text NOT NULL,
  `3_to` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
--
--

CREATE TABLE `net_diff_fact` (
  `1_primary_key` text NOT NULL,
  `2_component` text NOT NULL,
  `3_label` text NOT NULL,
  `4_interval_lower` float NOT NULL,
  `5_interval_upper` float NOT NULL,
  `6_t_lower` text NOT NULL,
  `7_t_upper` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
-- --------------------------------------------------------

--
-- Índices para tablas volcadas
--
--
-- Indices de la tabla `threshold_invocaciones`
--

ALTER TABLE `threshold_invocaciones`
  ADD PRIMARY KEY (`1_primary_key`(40));
--
-- Indices de la tabla `threshold_transferencias`
--

ALTER TABLE `threshold_transferencias`
  ADD PRIMARY KEY (`1_primary_key`(40));
--
-- Indices de la tabla `transferencias`
--

ALTER TABLE `transferencias`
  ADD PRIMARY KEY (`1_primary_key`(40));
--
-- Indices de la tabla `invocaciones`
--

ALTER TABLE `invocaciones`
  ADD PRIMARY KEY (`1_primary_key`(40));
--
-- Indices de la tabla `contracts_created`
--

ALTER TABLE `contracts_created`
  ADD PRIMARY KEY (`1_primary_key`(40));
--
-- Indices de la tabla `threshold_degree_in`
--

ALTER TABLE `threshold_degree_in`
  ADD PRIMARY KEY (`1_primary_key`(40));
--
-- Indices de la tabla `threshold_degree_out`
--

ALTER TABLE `threshold_degree_out`
  ADD PRIMARY KEY (`1_primary_key`(40));
--
-- Indices de la tabla `threshold_gasPrice_in`
--

ALTER TABLE `threshold_gasPrice_in`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `threshold_gasPrice_out`
--

ALTER TABLE `threshold_gasPrice_out`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `threshold_balance_out`
--

ALTER TABLE `threshold_balance_out`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `threshold_balance_in`
--

ALTER TABLE `threshold_balance_in`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `is_owner`
--

ALTER TABLE `is_owner`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `invoke`
--

ALTER TABLE `invoke`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `hyp_same_person`
--

ALTER TABLE `hyp_same_person`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `edge`
--
ALTER TABLE `edge`
  ADD PRIMARY KEY (`1_primary_key`(40)),
  ADD KEY `from_index` (`2_from`(40)),
  ADD KEY `to_index` (`3_to`(40)) USING BTREE;

--
-- Indices de la tabla `hyp_malicious`
--
ALTER TABLE `hyp_malicious`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `mapping`
--
ALTER TABLE `mapping`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `net_diff_fact`
--
ALTER TABLE `net_diff_fact`
  ADD PRIMARY KEY (`1_primary_key`(40)),
  ADD KEY `component_index` (`2_component`(40)),
  ADD KEY `label_index` (`3_label`(40)),
  ADD KEY `interval_lower_index` (`4_interval_lower`),
  ADD KEY `interval_upper_index` (`5_interval_upper`),
  ADD KEY `t_lower_index` (`6_t_lower`(40)),
  ADD KEY `t_upper_index` (`7_t_upper`(40));

--
-- Indices de la tabla `node`
--
ALTER TABLE `node`
  ADD PRIMARY KEY (`1_primary_key`(40)),
  ADD UNIQUE KEY `node_id_index` (`2_id`(40));

--
-- Indices de la tabla `null_info`
--
ALTER TABLE `null_info`
  ADD PRIMARY KEY (`1_primary_key`(40));


COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
