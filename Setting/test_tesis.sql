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

CREATE TABLE `is_smart_contract` (
  `1_primary_key` text NOT NULL,
  `2_address` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------
--
--
--

CREATE TABLE `is_eoa` (
  `1_primary_key` text NOT NULL,
  `2_address` text NOT NULL
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
-- Indices de la tabla `is_smart_contract`
--

ALTER TABLE `is_smart_contract`
  ADD PRIMARY KEY (`1_primary_key`(40));

--
-- Indices de la tabla `is_eoa`
--

ALTER TABLE `is_eoa`
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
