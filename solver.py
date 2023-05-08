# --------------------- LLIBRERIES ----------------------------------

import pandas as pd
import numpy as np
from scipy.optimize import minimize, LinearConstraint, NonlinearConstraint, shgo

# --------------------- CONFIGURACIÓ ----------------------------------

# ------------- Constraints d'optimització -------------
N_min_fulls_caixa_sense_castella = 1550*0.50        # Nombre mínim de fulls din-A3 que ha de contenir la caixa, excloent els exàmens addicionals de castellà
N_max_fulls_caixa_sense_castella = 1550*0.78     # Nombre màxim de fulls din-A3 que ha de contenir la caixa, excloent els exàmens addicionals de castellà

# ------------- Solvers -------------
# --- Solvers de Mínims Locals ---
# 'COBYLA'       --> Solver de Mínim Local, específic per problemes amb només inequacions. Ràpid. Molt depenent de x0 inicial. No necessita Jacobià 
# 'SLSQP'        --> Solver de Mínim Local, per problemes amb inequacions i equacions. Ràpid. Molt depenent de x0 inicial. Utilitza Jacobià
# 'trust-constr' --> Solver de Mínim Local, per problemes amb inequacions i equacions. Lent. Depenent de x0 inicial. Troba millors mínims locals. Utilitza Jacobià i Hessià

# --- Solvers de Mínim Global ---
# 'SHGO' (NO UTILITZAR)        --> Solver de Mínim Global. Extremadament lent. Torna el mínim global i també altres mínims locals. 

solver = 'SLSQP' 

# --- x0 ---
x0_option = 1

# --- x0_option = 1 ---
fulls_per_caixa_aprox = N_max_fulls_caixa_sense_castella     # Nombre aproximat de fulls din-A3 que ha de contenir la caixa, excloent els exàmens addicionals de castellà

# --- x0_option = 2 --- x0_option = 3 ---
x_mig = 1

# --------------------- FI CONFIGURACIÓ ----------------------------------


# Funció per optimitzar una caixa
def optimizar_caixa(caixa):

    matrix = caixa['Previsió amb marge'].to_numpy()
    N, M = matrix.shape
    k = np.array(list(caixa['Fulls per assignatura'].values()))

    # Funció objectiu
    def f(X,k=k):
        return sum(X[N:N+M])*np.inner(X[:N],k)

    # Jacobian de la funció objectiu
    def J(X,k=k):
        L = len(X)
        J = np.zeros(L)
        for j in range(L):
            if j < N:
                J[j]=k[j]*sum(X[N:N+M])
            else:
                J[j] = np.inner(X[:N],k)
        return J

    # Hessian de la funció objectiu
    def H(X,k=k):
        L = len(X)
        H = np.zeros([L,L])
        for i in range(L):
            for j in range(L):
                if i < N and j >= N:
                    H[i,j] = k[i]
                elif i >= N and j <N:
                    H[i,j] = k[j]
        return H

    # ----'COBYLA' -----
    if solver == 'COBYLA':
        # Constraints
        gs_1 = []  # Llista d'inequacions examens previstos
        gs_2 = []  # Llista d'inequacions del nombre mínim de fulls per caixa
        gs_3 = []  # Llista d'inequacions del nombre màxim de fulls per caixa
        gs_4 = []  # Llista d'inequacions dels Bounds (s'han de fer en inequació perquè COBYLA no accepta variable BOUNDS)

        # Inequacions d'examnes
        for i in range(N):
            for j in range(M):
                def g(X,matrix=matrix,i=i,j=j):   # To avoid Late Binding
                    n = np.array(X[:N])
                    x = np.array(X[N:N+M])
                    return n[i]*x[j]-matrix[i,j]
                gs_1.append(g)

        # Inequacions del nombre mínim i màxim de fulls per caixa
        # Inequació del nombre mínim de fulls per caixa
        def g(X, k=k, N_min_fulls_caixa_sense_castella=N_min_fulls_caixa_sense_castella):      # To avoid Late Binding
            return (np.inner(X[:N],k)-N_min_fulls_caixa_sense_castella)
        gs_2.append(g)
        
        # Inequació del nombre màxim de fulls per caixa
        def g(X, k=k, N_max_fulls_caixa_sense_castella=N_max_fulls_caixa_sense_castella):      # To avoid Late Binding
            return -(np.inner(X[:N],k)-N_max_fulls_caixa_sense_castella)
        gs_3.append(g)

        # Inequacions dels Bounds (s'han de fer en inequació perquè COBYLA no accepta variable BOUNDS)
        for i in range(N+M):    # Para cada valor de X
            # Lower Bound
            def g(X,i=i):      # To avoid Late Binding
                return X[i]
            gs_4.append(g)

        constr_1 = []   # Llista de diccionaris 1
        constr_2 = []   # Llista de diccionaris 2
        constr_3 = []   # Llista de diccionaris 3
        constr_4 = []   # Llista de diccionaris 4

        for g in gs_1:
            constr_1.append(dict(type='ineq', fun=g, args=(matrix,)))
        
        for g in gs_2:
            constr_2.append(dict(type='ineq', fun=g, args=(k,N_min_fulls_caixa_sense_castella,)))

        for g in gs_3:
            constr_3.append(dict(type='ineq', fun=g, args=(k,N_max_fulls_caixa_sense_castella,)))

        for g in gs_4:
            constr_4.append(dict(type='ineq', fun=g))

        constraints_list = constr_1 + constr_2 + constr_3 + constr_4


    # ----'SLSQP' or 'SHGO' -----
    if solver == 'SLSQP' or 'SHGO':
        # Bounds
        bounds = [(0, np.inf),]*(N+M)
        
        # Constraints
        # Inequacions
        gs_1 = []  # Llista d'inequacions examens previstos
        gs_2 = []  # Llista d'inequacions del nombre mínim de fulls per caixa
        gs_3 = []  # Llista d'inequacions del nombre màxim de fulls per caixa

        # Inequacions d'examnes
        for i in range(N):
            for j in range(M):
                def g(X,matrix=matrix,i=i,j=j):   # To avoid Late Binding
                    n = np.array(X[:N])
                    x = np.array(X[N:N+M])
                    return n[i]*x[j]-matrix[i,j]
                gs_1.append(g)

        # Inequacions del nombre mínim i màxim de fulls per caixa
        # Inequació del nombre mínim de fulls per caixa
        def g(X, k=k, N_min_fulls_caixa_sense_castella=N_min_fulls_caixa_sense_castella):      # To avoid Late Binding
            return (np.inner(X[:N],k)-N_min_fulls_caixa_sense_castella)
        gs_2.append(g)
        
        # Inequació del nombre màxim de fulls per caixa
        def g(X, k=k, N_max_fulls_caixa_sense_castella=N_max_fulls_caixa_sense_castella):      # To avoid Late Binding
            return -(np.inner(X[:N],k)-N_max_fulls_caixa_sense_castella)
        gs_3.append(g)

        constr_1 = []   # Llista de diccionaris 1
        constr_2 = []   # Llista de diccionaris 2
        constr_3 = []   # Llista de diccionaris 3

        for g in gs_1:
            constr_1.append(dict(type='ineq', fun=g, args=(matrix,)))
        
        for g in gs_2:
            constr_2.append(dict(type='ineq', fun=g, args=(k,N_min_fulls_caixa_sense_castella,)))

        for g in gs_3:
            constr_3.append(dict(type='ineq', fun=g, args=(k,N_max_fulls_caixa_sense_castella,)))

        constraints_list = constr_1 + constr_2 + constr_3 


    # ---- 'trust-constr' -----
    if solver == 'trust-constr':
        # Bounds
        bounds = [(0, np.inf),]*(N+M)

        # Constraints
        constraints_list = []

        # Linr Coeanstraints
        lb_linear_cons = np.array([N_min_fulls_caixa_sense_castella])
        ub_linear_cons = np.array([N_max_fulls_caixa_sense_castella])

        A_linear_cons = np.zeros((1,N+M))
        for i in range(N+M):
            if i < N:      # valors n
                A_linear_cons[0,i] = k[i]
            else:          # valors x
                A_linear_cons[0,i] = 0

        constraints_list.append(LinearConstraint(A=A_linear_cons, lb=lb_linear_cons, ub=ub_linear_cons))

        # Non-Linear Constraints
        for i in range(N):
            for j in range(M):

                def cons_f(X, i=i, j=j):   # To avoid Late Binding
                    return X[i]*X[N+j]
                
                def cons_J(X, i=i, j=j):
                    J = np.zeros(len(X))
                    J[i] = X[N+j]
                    J[N+j] = X[i]
                    return J
                    
                def cons_H(X, v, i=i, j=j):
                    H = np.zeros([len(X),len(X)])
                    H[i,N+j] = 1
                    H[N+j,i] = 1
                    return v[0]*H
            
                constraints_list.append(NonlinearConstraint(cons_f, matrix[i,j], np.inf, jac=cons_J, hess=cons_H))


    # Aproximació de x0

    if x0_option == 1:

        maxs_materia = np.array(caixa['Previsió amb marge'].stack().groupby('NOM_MATERIA').max()).ravel()
        perc_materia = maxs_materia / sum(maxs_materia)

        examens_per_caixa_aprox = fulls_per_caixa_aprox / np.inner(perc_materia,k)

        n_0 = np.ceil(perc_materia * examens_per_caixa_aprox).astype(int)

        x_0 = np.zeros(M)
        for k in range(M):
            x_0[k] = max(np.ceil(matrix[:,k]/n_0))

        x0 = list(n_0)+list(x_0)

    elif x0_option == 2:

        means_materia = np.array(caixa['Previsió amb marge'].stack().groupby('NOM_MATERIA').mean()).ravel()
        n_0 = np.ceil(means_materia/x_mig)
        
        x_0 = []
        for trib in caixa['Previsió amb marge'].columns:
            prev_trib = np.array(caixa['Previsió amb marge'][trib])
            
            x_max = 0
            for i in range(N):
                if np.ceil(prev_trib[i] / n_0[i]) > x_max:
                    x_max = np.ceil(prev_trib[i] / n_0[i])

            x_0.append(x_max)

        x0 = list(n_0)+list(x_0)

    elif x0_option == 3:

        maxs_materia = np.array(caixa['Previsió amb marge'].stack().groupby('NOM_MATERIA').max()).ravel()
        n_0 = np.ceil(maxs_materia/x_mig)
        
        x_0 = []
        for trib in caixa['Previsió amb marge'].columns:
            prev_trib = np.array(caixa['Previsió amb marge'][trib])
            
            x_max = 0
            for i in range(N):
                if np.ceil(prev_trib[i] / n_0[i]) > x_max:
                    x_max = np.ceil(prev_trib[i] / n_0[i])

            x_0.append(x_max)

        x0 = list(n_0)+list(x_0)

    # Solver ('COBYLA' or 'SLSQP' or 'trust-constr' or 'SHGO')
    if solver == 'COBYLA':
        result = minimize(f, x0, method=solver, constraints=constraints_list)
    elif solver == 'SLSQP':
        result = minimize(f, x0, method=solver, bounds=bounds, jac=J, constraints=constraints_list)
    elif solver == 'trust-constr':
        result = minimize(f, x0, method=solver, jac=J, hess=H, constraints=constraints_list, bounds=bounds)
    elif solver == 'SHGO':
        options = {'jac': J, 'hess':H}
        result = shgo(f, bounds=bounds, constraints=constraints_list, options=options)

    # Resultats arrodonits a enter
    result_ceil = np.ceil(result.x).astype(int)
    n = np.array(result_ceil[:N])
    x = np.array(result_ceil[N:N+M])

    return n,x